use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{
    BlockProviderExt, BlockSubscriberExt, ColdBootType, GcSubscriber, MetricsSubscriber,
    PsSubscriber, ShardStateApplier,
};
use tycho_core::blockchain_rpc::NoopBroadcastListener;
use tycho_core::global_config::GlobalConfig;
use tycho_core::node::{NodeBase, NodeBaseConfig, NodeKeys};
use tycho_rpc::{RpcConfig, RpcEndpoint, RpcState};
use tycho_toncenter::api;
use tycho_util::cli;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::LoggerConfig;
use tycho_util::cli::metrics::MetricsConfig;
use tycho_util::futures::JoinTask;
use tycho_util::serde_helpers::load_json_from_file;

/// Run a Tycho node.
#[derive(Parser)]
pub struct Cmd {
    /// dump the template of the zero state config
    #[clap(
        short = 'i',
        long,
        conflicts_with_all = ["config", "global_config", "keys", "logger_config", "import_zerostate"]
    )]
    pub init_config: Option<PathBuf>,

    /// overwrite the existing config
    #[clap(short, long)]
    pub force: bool,

    /// path to the node config
    #[clap(long, required_unless_present = "init_config")]
    pub config: Option<PathBuf>,

    /// path to the global config
    #[clap(long, required_unless_present = "init_config")]
    pub global_config: Option<PathBuf>,

    /// path to the node keys
    #[clap(long, required_unless_present = "init_config")]
    pub keys: Option<PathBuf>,

    /// path to the logger config
    #[clap(long)]
    pub logger_config: Option<PathBuf>,

    /// list of zerostate files to import
    #[clap(long)]
    pub import_zerostate: Option<Vec<PathBuf>>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        if let Some(config_path) = self.init_config {
            if config_path.exists() && !self.force {
                anyhow::bail!("config file already exists, use --force to overwrite");
            }

            let config = NodeConfig::default();
            std::fs::write(config_path, serde_json::to_string_pretty(&config).unwrap())?;
            return Ok(());
        }

        let node_config: NodeConfig =
            load_json_from_file(self.config.as_ref().context("no config")?)
                .context("failed to load node config")?;

        cli::logger::init_logger(&node_config.logger_config, self.logger_config.clone())?;
        cli::logger::set_abort_with_tracing();

        node_config.threads.init_global_rayon_pool().unwrap();
        node_config
            .threads
            .build_tokio_runtime()?
            .block_on(cli::signal::run_or_terminate(self.run_impl(node_config)))
    }

    async fn run_impl(self, node_config: NodeConfig) -> Result<()> {
        if let Some(metrics) = &node_config.metrics {
            // TODO: Make `async` or remove a tokio dependency from it.
            tycho_util::cli::metrics::init_metrics(metrics)?;
        }

        // Build node.
        let keys = NodeKeys::load_or_create(self.keys.unwrap())?;
        let global_config = GlobalConfig::from_file(self.global_config.unwrap())
            .context("failed to load global config")?;
        let public_ip = cli::resolve_public_ip(node_config.base.public_ip).await?;
        let public_addr = SocketAddr::new(public_ip, node_config.base.port);

        let node = NodeBase::builder(&node_config.base, &global_config)
            .init_network(public_addr, &keys.as_secret())?
            .init_storage()
            .await?
            .init_blockchain_rpc(NoopBroadcastListener, NoopBroadcastListener)?
            .build()?;

        // Sync node.
        let init_block_id = node
            .init(ColdBootType::LatestPersistent, self.import_zerostate, None)
            .await?;
        node.update_validator_set_from_shard_state(&init_block_id)
            .await?;

        // Build RPC.
        let _rpc_task;
        let (rpc_block_subscriber, rpc_state_subscriber) = {
            let config = &node_config.rpc;

            let rpc_state = RpcState::builder()
                .with_config(config.clone())
                .with_storage(node.core_storage.clone())
                .with_blockchain_rpc_client(node.blockchain_rpc_client.clone())
                .with_zerostate_id(node.global_config.zerostate)
                .build()?;

            rpc_state.init(&init_block_id).await?;

            let toncenter_routes = axum::Router::new()
                .nest("/toncenter/v2", api::toncenter_v2::router())
                .nest("/toncenter/v3", api::toncenter_v3::router());

            let endpoint = RpcEndpoint::builder()
                .with_custom_routes(toncenter_routes)
                .bind(rpc_state.clone())
                .await
                .context("failed to setup RPC server endpoint")?;

            tracing::info!(listen_addr = %config.listen_addr, "RPC server started");
            _rpc_task = JoinTask::new(async move {
                if let Err(e) = endpoint.serve().await {
                    tracing::error!("RPC server failed: {e:?}");
                }
                tracing::info!("RPC server stopped");
            });

            rpc_state.split()
        };

        // Build strider.
        let archive_block_provider = node.build_archive_block_provider();
        let storage_block_provider = node.build_storage_block_provider();
        let blockchain_block_provider = node
            .build_blockchain_block_provider()
            .with_fallback(archive_block_provider.clone());

        let gc_subscriber = GcSubscriber::new(node.core_storage.clone());
        let ps_subscriber = PsSubscriber::new(node.core_storage.clone());
        let block_strider = node.build_strider(
            archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
            (
                ShardStateApplier::new(
                    node.core_storage.clone(),
                    (rpc_state_subscriber, ps_subscriber),
                ),
                rpc_block_subscriber,
                node.validator_resolver().clone(),
                MetricsSubscriber,
            )
                .chain(gc_subscriber),
        );

        // Run block strider
        tracing::info!("block strider started");
        block_strider.run().await?;
        tracing::info!("block strider finished");

        Ok(())
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct NodeConfig {
    #[serde(flatten)]
    base: NodeBaseConfig,
    threads: ThreadPoolConfig,
    logger_config: LoggerConfig,
    metrics: Option<MetricsConfig>,
    rpc: RpcConfig,
}
