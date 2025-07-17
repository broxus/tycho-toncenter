use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_core::storage::CoreStorage;
use tycho_rpc::RpcState;
use tycho_storage::StorageContext;
use tycho_types::models::BlockId;
use tycho_types::prelude::*;
use tycho_util::FastDashMap;
use tycho_util::config::PartialConfig;

use self::interface::InterfaceType;
use self::ops::sync_after_boot::{
    FullSyncParams, SyncOutput, sync_after_boot_full, sync_after_boot_simple,
};
use self::repo::TokensRepo;

pub mod db;
pub mod interface;
pub mod models;
pub mod repo;
pub mod util;

mod ops {
    pub mod sync_after_boot;
}

const SUBDIR: &str = "toncenter";

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(default)]
pub struct TonCenterRpcConfig {
    #[important]
    pub force_reindex: bool,
    pub sync_group_count: usize,
}

impl Default for TonCenterRpcConfig {
    fn default() -> Self {
        Self {
            force_reindex: false,
            sync_group_count: 10,
        }
    }
}

#[derive(Clone)]
pub struct TonCenterRpcState {
    inner: Arc<Inner>,
}

impl TonCenterRpcState {
    pub async fn new(
        context: StorageContext,
        rpc_state: RpcState,
        core_storage: CoreStorage,
        config: TonCenterRpcConfig,
    ) -> Result<Self> {
        let dir = context
            .root_dir()
            .create_subdir(SUBDIR)
            .context("failed to create toncenter subdirectory")?;

        let tokens = TokensRepo::open(dir.path().join("tokens.db3"))
            .await
            .context("failed to create tokens repo")?;

        Ok(Self {
            inner: Arc::new(Inner {
                known_interfaces: Default::default(),
                core_storage,
                rpc_state,
                tokens,
                config,
            }),
        })
    }

    pub fn tokens(&self) -> &TokensRepo {
        &self.inner.tokens
    }

    pub fn rpc_state(&self) -> &RpcState {
        &self.inner.rpc_state
    }

    pub async fn sync_after_boot(&self, init_mc_block: &BlockId) -> Result<()> {
        let force_reindex = self.inner.config.force_reindex;
        let reindex_completed_at = self.inner.tokens.get_reindex_completion_block_id().await?;
        if let Some(prev_reindex_at) = &reindex_completed_at
            && force_reindex
        {
            tracing::info!(%prev_reindex_at);
        }

        let SyncOutput { known_interfaces } = if force_reindex || reindex_completed_at.is_none() {
            sync_after_boot_full(
                init_mc_block,
                &self.inner.core_storage,
                &self.inner.rpc_state,
                &self.inner.tokens,
                FullSyncParams {
                    sync_group_count: self.inner.config.sync_group_count,
                },
            )
            .await?
        } else {
            sync_after_boot_simple(&self.inner.tokens).await?
        };

        // TODO: Optimize.
        for (code_hash, interface) in known_interfaces.into_iter() {
            self.inner.known_interfaces.insert(code_hash, interface);
        }

        // Done
        Ok(())
    }
}

impl axum::extract::FromRef<TonCenterRpcState> for RpcState {
    #[inline]
    fn from_ref(input: &TonCenterRpcState) -> Self {
        input.inner.rpc_state.clone()
    }
}

struct Inner {
    known_interfaces: FastDashMap<HashBytes, InterfaceType>,
    core_storage: CoreStorage,
    rpc_state: RpcState,
    tokens: TokensRepo,
    config: TonCenterRpcConfig,
}

// TEMP
impl BlockSubscriber for TonCenterRpcState {
    type Prepared = ();
    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        _cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(async move { Ok(()) })
    }
}
