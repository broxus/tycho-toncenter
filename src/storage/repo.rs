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
            .build(SqliteConnectionManager::file(path).with_flags(
                OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ))
            .await
            .context("failed to create readers pool")?;

        Ok(Self { writer, readers })
    }

    pub async fn insert_jetton_masters(&self, items: Vec<JettonMaster>) -> Result<usize> {
        let values = tuple_list(items.len(), JettonMaster::COLUMN_COUNT);

        self.writer
            .dispatch(move |conn| {
                let mut stmt = QueryBuffer::with(|sql| {
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
                    .unwrap();
                    tracing::trace!(sql);

                    conn.prepare(sql)
                })?;

                let rows_affected = stmt.execute(params_from_iter(
                    items.iter().flat_map(|item| item.as_columns_iter()),
                ))?;
                tracing::debug!(rows_affected, "inserted jetton masters");

                Ok::<_, anyhow::Error>(rows_affected)
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

        let mut filter_query = String::new();
        let master_addresses_params = 'filter: {
            if let Some(addresses) = &params.master_addresses {
                if let Some(filter) = array_filter_params("J.address", addresses.len()) {
                    filter_query = filter;
                    break 'filter addresses.as_slice();
                }
            }
            &[]
        };

        let admin_address_params = 'filter: {
            if let Some(addresses) = &params.admin_addresses {
                if let Some(filter) = array_filter_params("J.admin_address", addresses.len()) {
                    filter_query = format!("{filter_query} AND {filter}");
                    break 'filter addresses.as_slice();
                }
            }
            &[]
        };

        if !filter_query.is_empty() {
            filter_query = format!("WHERE {filter_query}");
        }

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
