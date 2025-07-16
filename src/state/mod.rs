use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_core::storage::CoreStorage;
use tycho_rpc::RpcState;
use tycho_storage::StorageContext;
use tycho_types::models::BlockId;
use tycho_types::prelude::*;
use tycho_util::FastDashMap;

use self::interface::InterfaceType;
use self::ops::sync_after_boot::{SyncOutput, sync_after_boot};
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

#[derive(Clone)]
pub struct TonCenterRpcState {
    inner: Arc<Inner>,
}

impl TonCenterRpcState {
    pub async fn new(
        context: StorageContext,
        rpc_state: RpcState,
        core_storage: CoreStorage,
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
            }),
        })
    }

    pub fn tokens(&self) -> &TokensRepo {
        &self.inner.tokens
    }

    pub fn rpc_state(&self) -> &RpcState {
        &self.inner.rpc_state
    }

    pub async fn sync_after_boot(&self, init_mc_block: &BlockId, _full: bool) -> Result<()> {
        let SyncOutput { known_interfaces } = sync_after_boot(
            init_mc_block,
            &self.inner.core_storage,
            &self.inner.rpc_state,
            &self.inner.tokens,
        )
        .await?;

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
