use std::fmt::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, params_from_iter};
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;
use tycho_util::FastHashMap;

use super::db::*;
use super::interface::InterfaceType;
use super::models::*;
use super::util::*;

#[derive(Clone)]
pub struct TokensRepo {
    writer: SqliteDispatcher,
    readers: SqlitePool,
}

impl TokensRepo {
    pub const VERSION: (usize, usize, usize) = (1, 0, 0);

    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        let connection = Connection::open(path).context("failed to open db on init")?;
        prepare_connection(&connection)?;
        let writer = SqliteDispatcher::spawn(connection);
        writer
            .dispatch(|conn| {
                match get_version(conn)? {
                    // NOTE: Add migrations here
                    Some(version) => {
                        tracing::info!(?version, "opened an existing tokens DB");
                        anyhow::ensure!(
                            version == Self::VERSION,
                            "unknown tokens DB version: {version:?}"
                        );
                    }
                    None => {
                        tracing::info!("opened an empty tokens DB");
                        conn.execute_batch(include_str!("./schema.sql"))?;
                    }
                }
                Ok(())
            })
            .await
            .context("failed to apply db schema")?;

        let readers = bb8::Pool::builder()
            .retry_connection(false)
            .build(
                SqliteConnectionManager::file(path)
                    .with_flags(
                        OpenFlags::SQLITE_OPEN_READ_ONLY
                            | OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )
                    .with_init(prepare_connection),
            )
            .await
            .context("failed to create readers pool")?;

        Ok(Self { writer, readers })
    }

    pub async fn with_transaction<F>(&self, f: F) -> Result<usize>
    where
        for<'a> F: FnOnce(&TokensRepoTransaction),
    {
        let tx = TokensRepoTransaction::default();
        f(&tx);
        self.write(tx).await
    }

    pub async fn write(&self, tx: TokensRepoTransaction) -> Result<usize> {
        let batches = tx.batches.into_inner().unwrap();
        if batches.is_empty() {
            return Ok(0);
        }

        self.writer
            .dispatch(move |conn| {
                let tx = conn.transaction()?;

                let mut affected_rows = 0usize;
                for batch in batches {
                    affected_rows += batch(&tx)?;
                }

                let started_at = Instant::now();
                tx.commit()?;
                tracing::warn!(
                    elapsed = %humantime::format_duration(started_at.elapsed()),
                    affected_rows,
                    "commit",
                );

                Ok(affected_rows)
            })
            .await
    }

    pub fn write_blocking(&self, tx: TokensRepoTransaction) -> Result<usize> {
        let batches = tx.batches.into_inner().unwrap();
        if batches.is_empty() {
            return Ok(0);
        }

        self.writer.dispatch_blocking(move |conn| {
            let tx = conn.transaction()?;

            let mut affected_rows = 0usize;
            for batch in batches {
                affected_rows += batch(&tx)?;
            }

            let started_at = Instant::now();
            tx.commit()?;
            tracing::warn!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                affected_rows,
                "commit",
            );

            Ok(affected_rows)
        })
    }

    pub async fn get_all_known_interfaces(&self) -> Result<FastHashMap<HashBytes, InterfaceType>> {
        let conn = self.readers.get().await?;
        let mut stmt = conn.prepare("SELECT code_hash,interface FROM known_interfaces")?;

        stmt.query_map((), |row| {
            Ok((
                SqlType::<HashBytes>::get(row, 0)?,
                SqlType::<InterfaceType>::get(row, 1)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()
        .map_err(Into::into)
    }

    // TODO: Return iterator itself.
    pub async fn get_jetton_masters(
        &self,
        params: GetJettonMastersParams,
    ) -> Result<Vec<JettonMaster>> {
        const COLUMNS: &str = "J.address, J.total_supply, J.mintable, J.admin_address, J.jetton_content, \
        J.wallet_code_hash, J.last_transaction_lt, J.code_hash, J.data_hash";
        const TABLE: &str = "jetton_masters as J";
        const ORDER_BY: &str = "rowid ASC";

        let mut filter_query = None;
        let master_addresses_params = add_opt_array_filter(
            &mut filter_query,
            "J.address",
            params.master_addresses.as_deref(),
        );
        let admin_address_params = add_opt_array_filter(
            &mut filter_query,
            "J.admin_address",
            params.admin_addresses.as_deref(),
        );
        let filter_query = skip_or_where(&filter_query);

        let conn = self.readers.get().await?;
        let mut stmt = QueryBuffer::with(|sql| {
            write!(
                sql,
                "SELECT {COLUMNS} FROM {TABLE} \
                {filter_query} \
                ORDER BY {ORDER_BY} \
                LIMIT {} OFFSET {}",
                params.limit, params.offset
            )
            .unwrap();
            tracing::trace!(sql);

            conn.prepare(sql)
        })?;

        stmt.query_map(
            params_from_iter(
                master_addresses_params
                    .iter()
                    .chain(admin_address_params)
                    .map(SqlType::wrap),
            ),
            |row| JettonMaster::try_from(row),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
    }

    pub async fn get_jetton_wallets(
        &self,
        params: GetJettonWalletsParams,
    ) -> Result<Vec<JettonWallet>> {
        const COLUMNS: &str = "J.address, J.balance, J.owner, J.jetton, \
        J.last_transaction_lt, J.code_hash, J.data_hash";
        const TABLE: &str = "jetton_wallets as J";

        let (sort_column, sort_order) = match params.order_by {
            None => ("rowid", "ASC"),
            Some(OrderJettonWalletsBy::Balance { reverse: false }) => ("J.balance", "ASC"),
            Some(OrderJettonWalletsBy::Balance { reverse: true }) => ("J.balance", "DESC"),
        };
        let mut order_by = None;

        let mut filter_query = None;
        let wallet_addresses = if let Some(addresses) = &params.wallet_addresses {
            order_by = Some("J.address ASC".to_owned());
            add_array_filter(&mut filter_query, "J.address", addresses)
        } else {
            &[]
        };
        let owner_addresses = if let Some(addresses) = &params.owner_addresses {
            order_by = Some(format!("J.owner, {sort_column} {sort_order}"));
            add_array_filter(&mut filter_query, "J.owner", addresses)
        } else {
            &[]
        };
        let jetton_addresses = if let Some(addresses) = &params.jetton_addresses {
            if addresses.len() == 1 {
                order_by = Some(format!("J.jetton, {sort_column} {sort_order}"));
            }
            add_array_filter(&mut filter_query, "J.jetton", addresses)
        } else {
            &[]
        };

        if params.exclude_zero_balance {
            add_raw_filter(&mut filter_query, "J.balance != x'00'");
        }
        let filter_query = skip_or_where(&filter_query);
        let order_by = order_by.unwrap_or_else(|| format!("{sort_column} {sort_order}"));

        let conn = self.readers.get().await?;
        let mut stmt = QueryBuffer::with(|sql| {
            write!(
                sql,
                "SELECT {COLUMNS} FROM {TABLE} \
                {filter_query} \
                ORDER BY {order_by} \
                LIMIT {} OFFSET {}",
                params.limit, params.offset
            )
            .unwrap();
            tracing::trace!(sql);

            conn.prepare(sql)
        })?;

        stmt.query_map(
            params_from_iter(
                wallet_addresses
                    .iter()
                    .chain(owner_addresses)
                    .chain(jetton_addresses)
                    .map(SqlType::wrap),
            ),
            |row| JettonWallet::try_from(row),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
    }
}

#[derive(Default)]
pub struct TokensRepoTransaction {
    // TODO: Use bumpalo.
    batches: Mutex<Vec<Box<TokensRepoTransactionFn>>>,
}

impl TokensRepoTransaction {
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<usize> + Send + 'static,
    {
        self.batches.lock().unwrap().push(Box::new(f))
    }

    pub fn insert_known_interfaces(&self, rows: Vec<KnownInterface>) {
        self.execute_rows_simple(rows, |sql, values| {
            write!(
                sql,
                "INSERT INTO known_interfaces (code_hash,interface,is_broken) \
                VALUES {values} \
                ON CONFLICT(code_hash) DO UPDATE SET \
                    is_broken = excluded.is_broken"
            )
        })
    }

    pub fn insert_jetton_masters(&self, rows: Vec<JettonMaster>) {
        self.execute_rows_simple(rows, |sql, values| {
            write!(
                sql,
                "INSERT INTO jetton_masters (address,total_supply,mintable,\
                admin_address,jetton_content,wallet_code_hash,last_transaction_lt,\
                code_hash,data_hash) \
                VALUES {values} \
                ON CONFLICT(address) DO UPDATE SET \
                    total_supply = excluded.total_supply, \
                    mintable = excluded.mintable, \
                    admin_address = excluded.admin_address, \
                    jetton_content = excluded.jetton_content, \
                    wallet_code_hash = excluded.wallet_code_hash, \
                    last_transaction_lt = excluded.last_transaction_lt, \
                    code_hash = excluded.code_hash, \
                    data_hash = excluded.data_hash \
                WHERE last_transaction_lt < excluded.last_transaction_lt",
            )
        })
    }

    pub fn remove_jetton_masters(&self, rows: Vec<StdAddr>) {
        self.execute_rows_simple(rows, |sql, values| {
            write!(
                sql,
                "DELETE FROM jetton_masters WHERE address IN ({values})"
            )
        });
    }

    pub fn insert_jetton_wallets(&self, rows: Vec<JettonWallet>) {
        self.execute_rows_simple(rows, |sql, values| {
            write!(
                sql,
                "INSERT INTO jetton_wallets (address,balance,owner,jetton,\
                last_transaction_lt,code_hash,data_hash) \
                VALUES {values} \
                ON CONFLICT(address) DO UPDATE SET \
                    balance = excluded.balance, \
                    owner = excluded.owner, \
                    jetton = excluded.jetton, \
                    last_transaction_lt = excluded.last_transaction_lt, \
                    code_hash = excluded.code_hash, \
                    data_hash = excluded.data_hash \
                WHERE last_transaction_lt < excluded.last_transaction_lt",
            )
        })
    }

    pub fn remove_jetton_wallets(&self, rows: Vec<StdAddr>) {
        self.execute_rows_simple(rows, |sql, values| {
            write!(
                sql,
                "DELETE FROM jetton_wallets WHERE address IN ({values})"
            )
        });
    }

    pub fn execute_rows_simple<T, F>(&self, rows: Vec<T>, mut fmt: F)
    where
        T: SqlColumnsRepr + KnownColumnCount + Send + 'static,
        F: FnMut(&mut String, &str) -> std::fmt::Result + Send + 'static,
    {
        let row_count = rows.len();
        if row_count == 0 {
            return;
        }

        let rows_per_batch = SQLITE_MAX_VARIABLE_NUMBER / T::COLUMN_COUNT;
        let batch_count = row_count / rows_per_batch;
        let tail_len = row_count % rows_per_batch;

        let started_at = Instant::now();

        let batch_values = (batch_count > 0)
            .then(T::batch_params_string)
            .unwrap_or_default();
        let tail_values = if tail_len > 0 {
            tuple_list(tail_len, T::COLUMN_COUNT)
        } else {
            Default::default()
        };

        tracing::trace!(
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "prepared param list"
        );

        self.execute(move |tx| {
            let mut rows = rows.iter();
            let mut execute = |n: usize, values: &str| {
                let mut stmt = QueryBuffer::with(|sql| {
                    fmt(sql, values).unwrap();
                    tracing::trace!(sql);
                    tx.prepare(sql)
                })?;
                stmt.execute(params_from_iter(
                    rows.by_ref()
                        .take(n)
                        .flat_map(|item| item.as_columns_iter()),
                ))
            };

            let mut affected_rows = 0usize;
            for _ in 0..batch_count {
                affected_rows += execute(rows_per_batch, batch_values)?;
            }
            if tail_len > 0 {
                affected_rows += execute(tail_len, &tail_values)?;
            }
            assert!(rows.next().is_none());
            Ok(affected_rows)
        });
    }
}

type TokensRepoTransactionFn =
    dyn FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<usize> + Send + 'static;

fn get_version(connection: &Connection) -> Result<Option<(usize, usize, usize)>> {
    connection.execute(
        "CREATE TABLE IF NOT EXISTS db_version (
            id      INTEGER NOT NULL PRIMARY KEY,
            major   INTEGER NOT NULL,
            minor   INTEGER NOT NULL,
            patch   INTEGER NOT NULL
        )",
        (),
    )?;

    let mut stmt = connection.prepare("SELECT major, minor, patch FROM db_version LIMIT 1")?;
    let mut versions = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

    let result = match versions.next() {
        Some(Ok(version)) => version,
        Some(Err(e)) => return Err(e.into()),
        None => return Ok(None),
    };

    let other_versions = versions.collect::<Vec<_>>();
    if !other_versions.is_empty() {
        anyhow::bail!("too many versions stored: {other_versions:?}");
    }
    Ok(Some(result))
}

fn prepare_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "PRAGMA journal_mode=WAL;\
        PRAGMA synchronous=normal;\
        PRAGMA journal_size_limit=6144000;\
        PRAGMA cache_size=10000;\
        PRAGMA temp_store = MEMORY;\
        PRAGMA mmap_size = 268435456;",
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tycho_storage::StorageContext;
    use tycho_types::cell::HashBytes;
    use tycho_types::models::StdAddr;

    use super::*;

    fn dumb_addr(byte: u8) -> StdAddr {
        StdAddr::new(0, HashBytes([byte; 32]))
    }

    #[tokio::test]
    async fn can_reopen() -> Result<()> {
        tycho_util::test::init_logger("can_reopen", "debug");

        let (context, _tmp_dir) = StorageContext::new_temp().await?;
        let path = context.root_dir().path().join("tokens.db3");
        _ = TokensRepo::open(&path).await?;
        _ = TokensRepo::open(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn token_masters_query_works() -> Result<()> {
        tycho_util::test::init_logger("token_masters_query_works", "trace");

        let (context, _tmp_dir) = StorageContext::new_temp().await?;
        let path = context.root_dir().path().join("tokens.db3");
        let repo = TokensRepo::open(path).await?;

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
        let affected_rows = repo
            .with_transaction(|tx| tx.insert_jetton_masters(to_insert.to_vec()))
            .await?;
        assert_eq!(affected_rows, to_insert.len());
        let affected_rows = repo
            .with_transaction(|tx| tx.insert_jetton_masters(to_insert.to_vec()))
            .await?;
        assert_eq!(affected_rows, 0);

        to_insert[1].last_transaction_lt += 1;
        let affected_rows = repo
            .with_transaction(|tx| tx.insert_jetton_masters(to_insert.to_vec()))
            .await?;
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

        let changed = repo
            .with_transaction(|tx| {
                tx.remove_jetton_masters([0x11, 0x22, 0x33].map(dumb_addr).into_iter().collect())
            })
            .await?;
        assert_eq!(changed, 3);

        let result = repo
            .get_jetton_masters(GetJettonMastersParams {
                master_addresses: None,
                admin_addresses: None,
                limit: NonZeroUsize::new(10).unwrap(),
                offset: 0,
            })
            .await?;
        assert_eq!(result, &to_insert[3..]);

        Ok(())
    }

    #[tokio::test]
    async fn token_wallets_query_works() -> Result<()> {
        tycho_util::test::init_logger("token_wallets_query_works", "trace");

        let (context, _tmp_dir) = StorageContext::new_temp().await?;
        let path = context.root_dir().path().join("tokens.db3");
        let repo = TokensRepo::open(path).await?;

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
        let affected_rows = repo
            .with_transaction(|tx| tx.insert_jetton_wallets(to_insert.to_vec()))
            .await?;
        assert_eq!(affected_rows, to_insert.len());
        let affected_rows = repo
            .with_transaction(|tx| tx.insert_jetton_wallets(to_insert.to_vec()))
            .await?;
        assert_eq!(affected_rows, 0);

        to_insert[1].last_transaction_lt += 1;
        let affected_rows = repo
            .with_transaction(|tx| tx.insert_jetton_wallets(to_insert.to_vec()))
            .await?;
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

        let changed = repo
            .with_transaction(|tx| {
                tx.remove_jetton_wallets([0x11, 0x22, 0x33].map(dumb_addr).into_iter().collect())
            })
            .await?;
        assert_eq!(changed, 3);

        to_insert.reverse();
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
        assert_eq!(result, &to_insert[3..]);

        Ok(())
    }
}
