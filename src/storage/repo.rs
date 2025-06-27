use std::fmt::Write;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, params_from_iter};

use super::db::*;
use super::models::*;
use super::util::*;

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

    pub async fn insert_jetton_masters(&self, rows: Vec<JettonMaster>) -> Result<usize> {
        self.insert_rows_simple(rows, |sql, values| {
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
        .await
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

    pub async fn insert_jetton_wallets(&self, rows: Vec<JettonWallet>) -> Result<usize> {
        self.insert_rows_simple(rows, |sql, values| {
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
        .await
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
            order_by = Some(format!("J.address ASC"));
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

    async fn insert_rows_simple<T, F>(&self, rows: Vec<T>, fmt: F) -> Result<usize>
    where
        T: SqlColumnsRepr + KnownColumnCount + Send + 'static,
        F: FnOnce(&mut String, String) -> std::fmt::Result + Send + 'static,
    {
        let values = tuple_list(rows.len(), T::COLUMN_COUNT);
        self.writer
            .dispatch(move |conn| {
                let mut stmt = QueryBuffer::with(|sql| {
                    fmt(sql, values).unwrap();
                    tracing::trace!(sql);
                    conn.prepare(sql)
                })?;
                stmt.execute(params_from_iter(
                    rows.iter().flat_map(|item| item.as_columns_iter()),
                ))
                .map_err(Into::into)
            })
            .await
    }
}

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

    let result;
    match versions.next() {
        Some(Ok(version)) => result = version,
        Some(Err(e)) => return Err(e.into()),
        None => return Ok(None),
    }

    let other_versions = versions.collect::<Vec<_>>();
    if !other_versions.is_empty() {
        anyhow::bail!("too many versions stored: {other_versions:?}");
    }
    Ok(Some(result))
}

fn prepare_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch("PRAGMA journal_mode=WAL")
}
