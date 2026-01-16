use std::net::{IpAddr, Ipv4Addr};

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{BlockProviderExt, MetricsSubscriber, ShardStateApplier};
use tycho_core::node::{LightNodeConfig, LightNodeContext, NodeBaseConfig};
use tycho_rpc::{RpcConfig, RpcEndpoint, RpcState};
use tycho_toncenter::api;
use tycho_toncenter::state::{TonCenterRpcConfig, TonCenterRpcState};
use tycho_util::cli;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::LoggerConfig;
use tycho_util::cli::metrics::MetricsConfig;
use tycho_util::config::PartialConfig;
use tycho_util::futures::JoinTask;

/// Run a Tycho node.
#[derive(Parser)]
pub struct Cmd {
    #[clap(flatten)]
    args: tycho_core::node::CmdRunArgs,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        self.args.init_config_or_run_light_node(async move |ctx| {
            let LightNodeContext::<NodeConfig> {
                node,
                boot_args,
                config,
                ..
            } = ctx;

            // Sync node.
            let init_block_id = node.init_ext(boot_args).await?;
            node.update_validator_set_from_shard_state(&init_block_id)
                .await?;

            // Prepare RPC state.
            let rpc_state = RpcState::builder()
                .with_config(config.rpc.base.clone())
                .with_storage(node.core_storage.clone())
                .with_blockchain_rpc_client(node.blockchain_rpc_client.clone())
                .with_zerostate_id(node.global_config.zerostate)
                .build()?;

            rpc_state
                .init(&init_block_id)
                .await
                .context("failed to init RPC state")?;

            let ext_rpc_state = TonCenterRpcState::new(
                node.storage_context.clone(),
                rpc_state.clone(),
                node.core_storage.clone(),
                config.rpc.toncenter.clone(),
            )
            .await
            .context("failed to create an extended RPC state")?;

            ext_rpc_state.sync_after_boot(&init_block_id).await?;

            // Bind RPC.

            let _rpc_task = {
                let toncenter_routes = axum::Router::new()
                    .nest("/toncenter/v2", api::toncenter_v2::router())
                    .nest("/toncenter/v3", api::toncenter_v3::router());

                let endpoint = RpcEndpoint::builder()
                    .with_custom_routes(toncenter_routes)
                    .bind(ext_rpc_state.clone())
                    .await
                    .context("failed to setup RPC server endpoint")?;

                tracing::info!(listen_addr = %config.rpc.base.listen_addr, "RPC server started");
                JoinTask::new(async move {
                    if let Err(e) = endpoint.serve().await {
                        tracing::error!("RPC server failed: {e:?}");
                    }
                    tracing::info!("RPC server stopped");
                })
            };

            let (rpc_block_subscriber, rpc_state_subscriber) = rpc_state.split();
            let (rpc_ext_block_subscriber, rpc_ext_state_subscriber) = ext_rpc_state.split();

            // Build strider.
            let archive_block_provider = node.build_archive_block_provider();
            let storage_block_provider = node.build_storage_block_provider();
            let blockchain_block_provider = node
                .build_blockchain_block_provider()
                .with_fallback(archive_block_provider.clone());

            let block_strider = node.build_strider(
                archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
                (
                    ShardStateApplier::new(
                        node.core_storage.clone(),
                        (rpc_state_subscriber, rpc_ext_state_subscriber),
                    ),
                    rpc_block_subscriber,
                    rpc_ext_block_subscriber,
                    node.validator_resolver().clone(),
                    MetricsSubscriber,
                ),
            );

            // Run block strider
            tracing::info!("block strider started");
            block_strider.run().await?;
            tracing::info!("block strider finished");

            Ok(())
        })
    }
}

#[allow(unused)]
#[derive(PartialConfig, Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct NodeConfig {
    #[partial]
    #[serde(flatten)]
    base: NodeBaseConfig,
    #[important]
    threads: ThreadPoolConfig,
    #[important]
    logger_config: LoggerConfig,
    #[important]
    metrics: Option<MetricsConfig>,
    #[partial]
    rpc: ExtRpcConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        let mut metrics = MetricsConfig::default();

        // NOTE: Simplify usage for docker by default.
        // Metrics don't expose anything sensible, and the exported
        // amount is not that big to fill the whole channel.
        metrics
            .listen_addr
            .set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        Self {
            base: NodeBaseConfig::default(),
            threads: ThreadPoolConfig::default(),
            logger_config: LoggerConfig::default(),
            metrics: Some(metrics),
            rpc: ExtRpcConfig::default(),
        }
    }
}

impl LightNodeConfig for NodeConfig {
    fn base(&self) -> &NodeBaseConfig {
        &self.base
    }

    fn threads(&self) -> &cli::config::ThreadPoolConfig {
        &self.threads
    }

    fn metrics(&self) -> Option<&cli::metrics::MetricsConfig> {
        self.metrics.as_ref()
    }

    fn logger(&self) -> Option<&cli::logger::LoggerConfig> {
        Some(&self.logger_config)
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
struct ExtRpcConfig {
    #[partial]
    #[serde(flatten)]
    base: RpcConfig,

    #[partial]
    toncenter: TonCenterRpcConfig,
}
