use anyhow::{Context, Result};
use tycho_storage::StorageContext;

pub use self::repo::TokensRepo;

pub mod models;
pub mod util;

mod db;
mod repo;

const SUBDIR: &str = "toncenter";

pub struct TonCenterStorage {
    tokens: TokensRepo,
}

impl TonCenterStorage {
    pub async fn new(context: StorageContext) -> Result<Self> {
        let dir = context
            .root_dir()
            .create_subdir(SUBDIR)
            .context("failed to create toncenter subdirectory")?;

        let tokens = TokensRepo::open(dir.path().join("tokens.db3"))
            .await
            .context("failed to create tokens repo")?;

        Ok(Self { tokens })
    }

    pub fn tokens(&self) -> &TokensRepo {
        &self.tokens
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use everscale_types::cell::HashBytes;
    use everscale_types::models::StdAddr;

    use super::*;
    use crate::storage::models::*;

    fn dumb_addr(byte: u8) -> StdAddr {
        StdAddr::new(0, HashBytes([byte; 32]))
    }

    #[tokio::test]
    async fn can_reopen() -> Result<()> {
        tycho_util::test::init_logger("can_reopen", "debug");

        let (context, _tmp_dir) = StorageContext::new_temp().await?;
        _ = TonCenterStorage::new(context.clone()).await?;
        _ = TonCenterStorage::new(context).await?;
        Ok(())
    }

    #[tokio::test]
    async fn token_masters_query_works() -> Result<()> {
        tycho_util::test::init_logger("token_masters_query_works", "trace");

        let (context, _tmp_dir) = StorageContext::new_temp().await?;
        let storage = TonCenterStorage::new(context.clone()).await?;

        let repo = storage.tokens();

        for _ in 0..3 {
            let result = repo
                .get_jetton_masters(GetJettonMastersParams {
                    master_addresses: Some(vec![dumb_addr(0x11), dumb_addr(0x22)]),
                    admin_addresses: Some(vec![dumb_addr(0x11)]),
                    limit: NonZeroUsize::new(10).unwrap(),
                    offset: 0,
                })
                .await?;
            assert!(result.is_empty());
        }

        let mut to_insert = [0x11, 0x22, 0x33, 0x44, 0x55].map(|addr| JettonMaster {
            address: dumb_addr(addr),
            total_supply: (addr as u32 * 123u32).into(),
            mintable: true,
            admin_address: Some(dumb_addr(0x22)),
            jetton_content: None,
            wallet_code_hash: HashBytes::ZERO,
            last_transaction_lt: 123,
            code_hash: HashBytes::ZERO,
            data_hash: HashBytes::ZERO,
        });
        let affected_rows = repo.insert_jetton_masters(to_insert.to_vec()).await?;
        assert_eq!(affected_rows, to_insert.len());
        let affected_rows = repo.insert_jetton_masters(to_insert.to_vec()).await?;
        assert_eq!(affected_rows, 0);

        to_insert[1].last_transaction_lt += 1;
        let affected_rows = repo.insert_jetton_masters(to_insert.to_vec()).await?;
        assert_eq!(affected_rows, 1);

        for _ in 0..3 {
            let result = repo
                .get_jetton_masters(GetJettonMastersParams {
                    master_addresses: None,
                    admin_addresses: None,
                    limit: NonZeroUsize::new(10).unwrap(),
                    offset: 0,
                })
                .await?;
            assert_eq!(result, to_insert);
        }

        Ok(())
    }

    #[tokio::test]
    async fn token_wallets_query_works() -> Result<()> {
        tycho_util::test::init_logger("token_wallets_query_works", "trace");

        let (context, _tmp_dir) = StorageContext::new_temp().await?;
        let storage = TonCenterStorage::new(context.clone()).await?;

        let repo = storage.tokens();

        for _ in 0..3 {
            let result = repo
                .get_jetton_wallets(GetJettonWalletsParams {
                    wallet_addresses: None,
                    owner_addresses: Some(vec![dumb_addr(0x11), dumb_addr(0x22)]),
                    jetton_addresses: Some(vec![dumb_addr(0x11)]),
                    limit: NonZeroUsize::new(10).unwrap(),
                    offset: 0,
                    exclude_zero_balance: true,
                    order_by: None,
                })
                .await?;
            assert!(result.is_empty());
        }

        let mut to_insert = [0x11, 0x22, 0x33, 0x44, 0x55].map(|addr| JettonWallet {
            address: dumb_addr(addr),
            balance: (addr as u32 * 123u32).into(),
            owner: dumb_addr(0x22),
            jetton: dumb_addr(0x11),
            last_transaction_lt: 123,
            code_hash: Some(HashBytes::ZERO),
            data_hash: Some(HashBytes::ZERO),
        });
        let affected_rows = repo.insert_jetton_wallets(to_insert.to_vec()).await?;
        assert_eq!(affected_rows, to_insert.len());
        let affected_rows = repo.insert_jetton_wallets(to_insert.to_vec()).await?;
        assert_eq!(affected_rows, 0);

        to_insert[1].last_transaction_lt += 1;
        let affected_rows = repo.insert_jetton_wallets(to_insert.to_vec()).await?;
        assert_eq!(affected_rows, 1);

        for _ in 0..3 {
            let result = repo
                .get_jetton_wallets(GetJettonWalletsParams {
                    wallet_addresses: None,
                    owner_addresses: None,
                    jetton_addresses: None,
                    limit: NonZeroUsize::new(10).unwrap(),
                    offset: 0,
                    exclude_zero_balance: true,
                    order_by: None,
                })
                .await?;
            assert_eq!(result, to_insert);
        }

        to_insert.reverse();
        let result = repo
            .get_jetton_wallets(GetJettonWalletsParams {
                wallet_addresses: None,
                owner_addresses: None,
                jetton_addresses: None,
                limit: NonZeroUsize::new(10).unwrap(),
                offset: 0,
                exclude_zero_balance: true,
                order_by: Some(OrderJettonWalletsBy::Balance { reverse: true }),
            })
            .await?;
        assert_eq!(result, to_insert);

        let result = repo
            .get_jetton_wallets(GetJettonWalletsParams {
                wallet_addresses: None,
                owner_addresses: Some(vec![dumb_addr(0x22)]),
                jetton_addresses: None,
                limit: NonZeroUsize::new(10).unwrap(),
                offset: 0,
                exclude_zero_balance: true,
                order_by: Some(OrderJettonWalletsBy::Balance { reverse: true }),
            })
            .await?;
        assert_eq!(result, to_insert);

        Ok(())
    }
}
