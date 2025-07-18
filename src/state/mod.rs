use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{
    BlockSubscriber, BlockSubscriberContext, StateSubscriber, StateSubscriberContext,
};
use tycho_core::storage::CoreStorage;
use tycho_rpc::RpcState;
use tycho_storage::StorageContext;
use tycho_types::models::BlockId;
use tycho_util::config::PartialConfig;
use tycho_util::futures::JoinTask;

use self::ops::runtime_sync::RuntimeSyncState;
use self::ops::sync_after_boot::{
    FullSyncParams, SyncOutput, sync_after_boot_full, sync_after_boot_simple,
};
use self::repo::TokensRepo;

pub mod db;
pub mod models;
pub mod parser;
pub mod repo;
pub mod util;

mod ops {
    pub mod runtime_sync;
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

        let runtime = RuntimeSyncState::new(tokens.clone(), rpc_state.clone());

        Ok(Self {
            inner: Arc::new(Inner {
                core_storage,
                rpc_state,
                tokens,
                config,
                runtime,
            }),
        })
    }

    pub fn tokens(&self) -> &TokensRepo {
        &self.inner.tokens
    }

    pub fn rpc_state(&self) -> &RpcState {
        &self.inner.rpc_state
    }

    pub fn split(self) -> (TonCenterRpcBlockSubscriber, TonCenterRpcStateSubscriber) {
        (
            TonCenterRpcBlockSubscriber {
                inner: self.inner.clone(),
            },
            TonCenterRpcStateSubscriber { inner: self.inner },
        )
    }

    pub async fn sync_after_boot(&self, init_mc_block: &BlockId) -> Result<()> {
        let force_reindex = self.inner.config.force_reindex;
        let reindex_completed_at = {
            let reader = self.inner.tokens.read().await?;
            reader.get_reindex_completion_block_id()?
        };
        if let Some(prev_reindex_at) = &reindex_completed_at
            && force_reindex
        {
            tracing::info!(%prev_reindex_at);
        }

        let SyncOutput { cache } = if force_reindex || reindex_completed_at.is_none() {
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

        self.inner.runtime.cache().merge(cache);

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
    core_storage: CoreStorage,
    rpc_state: RpcState,
    tokens: TokensRepo,
    config: TonCenterRpcConfig,
    runtime: RuntimeSyncState,
}

pub struct TonCenterRpcBlockSubscriber {
    inner: Arc<Inner>,
}

impl BlockSubscriber for TonCenterRpcBlockSubscriber {
    type Prepared = JoinTask<Result<()>>;
    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ok(JoinTask::new(
            self.inner.runtime.handle_block(cx.block.clone()),
        ))
    }

    fn handle_block<'a>(
        &'a self,
        _: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(prepared)
    }
}

pub struct TonCenterRpcStateSubscriber {
    inner: Arc<Inner>,
}

impl StateSubscriber for TonCenterRpcStateSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(self.inner.runtime.handle_state(cx.state.clone()))
    }
}
