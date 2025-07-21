use std::collections::hash_map;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use futures_util::FutureExt;
use tokio::sync::Notify;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::ShardStateStuff;
use tycho_rpc::{GenTimings, RpcState};
use tycho_types::models::{Account, AccountStatus, BlockId, BlockIdShort, StdAddr};
use tycho_types::prelude::*;
use tycho_util::{FastDashMap, FastHashMap};
use tycho_vm::{OwnedCellSlice, SafeRc};

use crate::state::parser::{
    GetWalletAddressOutput, InterfaceCache, InterfaceParser, JettonMasterInterface, ParserOutput,
};
use crate::state::repo::{TokensRepo, TokensRepoTransaction};
use crate::util::tonlib_helpers::{RunGetterParams, SimpleExecutor};

#[derive(Clone)]
pub struct RuntimeSyncState {
    inner: Arc<Inner>,
}

struct Inner {
    block_received: Notify,
    parsed_blocks: FastDashMap<BlockIdShort, ParsedBlock>,
    parsed_blocks_len: AtomicUsize,
    cache: InterfaceCache,
    tokens: TokensRepo,
    rpc_state: RpcState,
}

impl RuntimeSyncState {
    pub fn new(tokens: TokensRepo, rpc_state: RpcState) -> Self {
        Self {
            inner: Arc::new(Inner {
                block_received: Default::default(),
                parsed_blocks: Default::default(),
                parsed_blocks_len: Default::default(),
                cache: Default::default(),
                tokens,
                rpc_state,
            }),
        }
    }

    pub fn cache(&self) -> &InterfaceCache {
        &self.inner.cache
    }

    pub fn handle_block(
        &self,
        block: BlockStuff,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let started_at = Instant::now();

        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.handle_block(block, started_at)).map(|res| {
            match res {
                Ok(res) => res,
                Err(e) => Err(e.into()),
            }
        })
    }

    pub async fn handle_state(&self, state: ShardStateStuff) -> Result<()> {
        let started_at = Instant::now();

        let block_id = *state.block_id();
        let parsed = loop {
            let block_received = self.inner.block_received.notified();
            if let Some((_, parsed)) = self.inner.parsed_blocks.remove(&block_id.as_short_id()) {
                self.inner.parsed_blocks_len.fetch_sub(1, Ordering::Relaxed);
                break parsed;
            }
            block_received.await;
        };
        anyhow::ensure!(
            block_id == parsed.block_id,
            "full block id mismatch: parsed_block_id={}, expected={block_id}",
            parsed.block_id,
        );

        if parsed.updated_accounts.is_empty() {
            return Ok(());
        }

        let Ok::<i8, _>(workchain) = block_id.shard.workchain().try_into() else {
            tracing::info!(shard = %block_id.shard, "skipped shard state for an unknown shard");
            return Ok(());
        };

        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner.handle_state(workchain, state, parsed, started_at)
        })
        .await?
    }
}

impl Inner {
    fn handle_block(&self, block: BlockStuff, started_at: Instant) -> Result<()> {
        let parsed = 'parse_block: {
            let extra = block.load_extra()?;
            let account_blocks = extra.account_blocks.load()?;

            if account_blocks.is_empty() {
                // No transactions in block.
                break 'parse_block ParsedBlock {
                    block_id: *block.id(),
                    updated_accounts: Default::default(),
                };
            }

            // Find account updates.
            let mut updated_accounts = FastHashMap::default();
            for item in account_blocks.iter() {
                let (address, _, account_block) = item?;

                let mut was_active = false;
                let mut is_active = false;

                // Process account transactions
                let mut first_tx = true;
                for item in account_block.transactions.values() {
                    let (_, tx_cell) = item?;

                    let tx = tx_cell.load()?;
                    // Update flags
                    if first_tx {
                        was_active = tx.orig_status == AccountStatus::Active;
                        first_tx = false;
                    }
                    is_active = tx.end_status == AccountStatus::Active;
                }

                // Check for update.
                let update = match (was_active, is_active) {
                    (false, false) => None,
                    (false, true) => Some(AccountUpdate::Deplyed),
                    (true, true) => Some(AccountUpdate::Changed),
                    (true, false) => Some(AccountUpdate::Removed),
                };

                if let Some(update) = update {
                    updated_accounts.insert(address, update);
                }
            }

            ParsedBlock {
                block_id: *block.id(),
                updated_accounts,
            }
        };

        tracing::info!(
            block_id = %block.id().as_short_id(),
            updated_accounts = parsed.updated_accounts.len(),
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "parsed block"
        );
        self.parsed_blocks.insert(block.id().as_short_id(), parsed);
        self.block_received.notify_waiters();
        self.parsed_blocks_len.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn handle_state(
        &self,
        workchain: i8,
        state: ShardStateStuff,
        parsed_block: ParsedBlock,
        started_at: Instant,
    ) -> Result<()> {
        let block_id = state.block_id().as_short_id();

        let config = self.rpc_state.get_unpacked_blockchain_config();
        let parser = InterfaceParser {
            cache: &self.cache,
            executor: SimpleExecutor {
                libraries: self.rpc_state.get_libraries(),
                raw_config: config.raw.clone(),
                unpacked_config: config.unpacked.clone(),
                timings: GenTimings {
                    gen_lt: state.as_ref().gen_lt,
                    gen_utime: state.as_ref().gen_utime,
                },
                modifiers: Default::default(),
            },
        };

        let total_accounts = parsed_block.updated_accounts.len();
        let mut total_known_interfaces = 0usize;
        let mut total_errors = 0usize;

        let accounts = state.as_ref().accounts.load()?;

        let mut to_remove = Vec::new();
        let mut new_interfaces = FastHashMap::default();
        let mut jetton_masters = Vec::new();
        let mut jetton_wallets = Vec::new();
        let mut unverified_jetton_wallets = Vec::new();

        // TODO: Split into batches (with at least N items in each).
        for (address, status) in parsed_block.updated_accounts {
            let address = StdAddr::new(workchain, address);

            // No need to parse account state if it was removed.
            if status == AccountUpdate::Removed {
                to_remove.push(address);
                continue;
            }

            let mut account = if let Some((_, account)) = accounts.get(address.address)?
                && let Some(account) = account.load_account()?
            {
                account
            } else {
                tracing::warn!(
                    %block_id,
                    %address,
                    "account should exist but not found in state"
                );
                total_errors += 1;
                continue;
            };

            let is_new = status == AccountUpdate::Deplyed;
            match parser.handle_account(&address, &mut account) {
                Ok(Some(ty)) => {
                    total_known_interfaces += 1;

                    match ty {
                        ParserOutput::JettonMaster(info) => {
                            if is_new {
                                new_interfaces.insert(info.code_hash, info.as_known_interface());
                            }
                            jetton_masters.push(info)
                        }
                        ParserOutput::JettonWallet(info) if is_new => {
                            unverified_jetton_wallets.push(info)
                        }
                        ParserOutput::JettonWallet(info) => {
                            jetton_wallets.push(info);
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => total_errors += 1,
            }
        }

        let mut with_unknown_master = 0usize;
        let mut with_failed_getter = 0usize;
        let mut with_mismatched_address = 0usize;
        let mut master_accounts_cache = FastHashMap::<StdAddr, Account>::default();

        // TODO: Split into batches (with at least N items in each).
        'outer: for info in unverified_jetton_wallets {
            if info.jetton.workchain != workchain {
                with_unknown_master += 1;
                continue;
            }

            let account = match master_accounts_cache.entry(info.jetton.clone()) {
                hash_map::Entry::Occupied(entry) => entry.into_mut(),
                hash_map::Entry::Vacant(entry) => 'addr: {
                    if let Some((_, state)) = accounts.get(info.jetton.address)?
                        && let Some(account) = state.load_account()?
                    {
                        break 'addr entry.insert(account);
                    }

                    with_unknown_master += 1;
                    continue 'outer;
                }
            };

            let owner = SafeRc::new_dyn_value(OwnedCellSlice::new_allow_exotic(
                CellBuilder::build_from(&info.owner).unwrap(),
            ));

            let Ok(output) = parser.executor.run_getter::<GetWalletAddressOutput>(
                account,
                RunGetterParams::new(JettonMasterInterface::get_wallet_address())
                    .with_args([owner]),
            ) else {
                // Getter failed.
                with_failed_getter += 1;
                continue;
            };

            if output.address != info.address {
                // Computed address mismatch.
                to_remove.push(info.address);
                with_mismatched_address += 1;
                continue;
            }

            new_interfaces.insert(info.code_hash, info.as_known_interface());
            jetton_wallets.push(info);
        }
        drop(master_accounts_cache);

        // TODO: Wait somewhere else?
        let tx = TokensRepoTransaction::default();
        tx.remove_contracts(to_remove);
        tx.insert_known_interfaces(new_interfaces.into_values().collect());
        tx.insert_jetton_masters(jetton_masters);
        tx.insert_jetton_wallets(jetton_wallets);
        let affected_rows = self
            .tokens
            .write_blocking(tx)
            .context("failed to write tokens info batch")?;

        tracing::info!(
            %block_id,
            total_accounts,
            total_known_interfaces,
            total_errors,
            with_unknown_master,
            with_failed_getter,
            with_mismatched_address,
            affected_rows,
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "inserted tokens info batch",
        );
        Ok(())
    }
}

struct ParsedBlock {
    block_id: BlockId,
    updated_accounts: FastHashMap<HashBytes, AccountUpdate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountUpdate {
    Deplyed,
    Changed,
    Removed,
}
