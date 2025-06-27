use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::StdAddr;
use futures_util::future::BoxFuture;
use num_bigint::BigUint;
use rand::Rng;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_rpc::RpcState;
use tycho_storage::StorageContext;

use self::models::JettonWallet;
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
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(async move {
            let info = cx.block.load_info()?;

            let new_wallets = {
                let mut rng = rand::thread_rng();
                (0..10000)
                    .map(|_| JettonWallet {
                        address: StdAddr::new(0, rng.r#gen()),
                        balance: BigUint::from(rng.r#gen::<u64>()),
                        owner: StdAddr::new(0, rng.r#gen()),
                        jetton: StdAddr::new(0, rng.r#gen()),
                        last_transaction_lt: info.start_lt,
                        code_hash: Some(rng.r#gen()),
                        data_hash: Some(rng.r#gen()),
                    })
                    .collect::<Vec<_>>()
            };

            let rows = self.inner.tokens.insert_jetton_wallets(new_wallets).await?;
            tracing::info!(rows, "inserted stub wallets");

            Ok(())
        })
    }
}
