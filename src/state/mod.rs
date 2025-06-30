use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_rpc::RpcState;
use tycho_storage::StorageContext;

pub use self::repo::TokensRepo;

pub mod models;
pub mod util;

mod db;
mod repo;

const SUBDIR: &str = "toncenter";

#[derive(Clone)]
pub struct TonCenterRpcState {
    inner: Arc<Inner>,
}

impl TonCenterRpcState {
    pub async fn new(context: StorageContext, rpc_state: RpcState) -> Result<Self> {
        let dir = context
            .root_dir()
            .create_subdir(SUBDIR)
            .context("failed to create toncenter subdirectory")?;

        let tokens = TokensRepo::open(dir.path().join("tokens.db3"))
            .await
            .context("failed to create tokens repo")?;

        Ok(Self {
            inner: Arc::new(Inner { rpc_state, tokens }),
        })
    }

    pub fn tokens(&self) -> &TokensRepo {
        &self.inner.tokens
    }

    pub fn rpc_state(&self) -> &RpcState {
        &self.inner.rpc_state
    }
}

impl axum::extract::FromRef<TonCenterRpcState> for RpcState {
    #[inline]
    fn from_ref(input: &TonCenterRpcState) -> Self {
        input.inner.rpc_state.clone()
    }
}

struct Inner {
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
