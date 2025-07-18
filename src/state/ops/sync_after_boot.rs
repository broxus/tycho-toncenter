use std::time::Instant;

use anyhow::{Context, Result};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::CoreStorage;
use tycho_rpc::{GenTimings, RpcState};
use tycho_types::models::{
    Account, BlockId, BlockchainConfigParams, DepthBalanceInfo, LibDescr, ShardAccount, ShardIdent,
    StdAddr,
};
use tycho_types::prelude::*;
use tycho_util::{FastDashMap, FastHashMap};
use tycho_vm::{OwnedCellSlice, SafeRc};

use crate::state::parser::{
    GetWalletAddressOutput, InterfaceCache, InterfaceParser, InterfaceParserBatch, InterfaceType,
    JettonMasterInterface,
};
use crate::state::repo::{TokensRepo, TokensRepoTransaction};
use crate::util::tonlib_helpers::{RunGetterParams, SimpleExecutor};

pub struct FullSyncParams {
    pub sync_group_count: usize,
}

pub struct SyncOutput {
    pub cache: InterfaceCache,
}

pub async fn sync_after_boot_simple(tokens: &TokensRepo) -> Result<SyncOutput> {
    tracing::info!("doing simple reindex");

    tokens
        .with_transaction(|tx| tx.remove_unused_interfaces())
        .await
        .context("failed to cleanup unused interfaces")?;

    let known_interfaces = tokens
        .read()
        .await?
        .get_all_known_interfaces()?
        .collect::<rusqlite::Result<FastDashMap<_, _>>>()?;

    Ok(SyncOutput {
        cache: InterfaceCache {
            known_interfaces,
            skip_code: Default::default(),
        },
    })
}

pub async fn sync_after_boot_full(
    init_mc_block: &BlockId,
    core_storage: &CoreStorage,
    rpc_state: &RpcState,
    tokens: &TokensRepo,
    params: FullSyncParams,
) -> Result<SyncOutput> {
    tracing::info!("doing full reindex");

    // Split virtual shards.
    let LoadedVirtualShards {
        virtual_shards,
        libraries,
        config,
        timings,
    } = load_virtual_shards(core_storage, rpc_state, init_mc_block)
        .await
        .context("failed to load virtual shards")?;

    tracing::info!("get known interfaces");

    // Load known code hashes cache.
    let known_interfaces = tokens
        .read()
        .await?
        .get_all_known_interfaces()?
        .collect::<rusqlite::Result<FastDashMap<_, _>>>()?;

    let executor = SimpleExecutor::new(config, libraries, timings)
        .context("failed to create a simple executor")?;

    let tokens = tokens.clone();
    let (tokens, jetton_masters, executor, cache) = tokio::task::spawn_blocking(move || {
        let tx = TokensRepoTransaction::default();
        tx.clear_contracts();
        tokens
            .write_blocking(tx)
            .context("failed to clear contracts")?;

        let cache = InterfaceCache {
            known_interfaces,
            skip_code: Default::default(),
        };

        let context = InitialSyncContext {
            parser: InterfaceParser {
                cache: &cache,
                executor,
            },
            tokens,
            jetton_master_accounts: Default::default(),
        };

        // TODO: Optimize.
        let mut groups = (0..params.sync_group_count)
            .map(|_| Vec::<(ShardIdent, ShardAccountsDict)>::new())
            .collect::<Box<[Vec<_>]>>();

        for (i, item) in virtual_shards.into_iter().enumerate() {
            groups[i % groups.len()].push(item);
        }

        std::thread::scope(|scope| {
            let context = &context;
            for group in groups {
                scope.spawn(move || {
                    for (shard_ident, accounts) in group {
                        if let Err(e) = context.process_batch(shard_ident, accounts) {
                            // TODO: Should we just unwrap here?
                            tracing::error!("FATAL, failed to process virtual shard: {e:?}");
                        }
                    }
                });
            }
        });

        Ok::<_, anyhow::Error>((
            context.tokens,
            context.jetton_master_accounts,
            context.parser.executor,
            cache,
        ))
    })
    .await??;

    tracing::info!("started verify");

    // Verify token wallets info.
    let reader = tokens.read_owned().await?;
    let tokens = tokio::task::spawn_blocking(move || {
        let started_at = Instant::now();

        let mut to_remove = Vec::new();

        let mut total_wallets = 0usize;
        let mut with_unknown_master = 0usize;
        let mut with_failed_getter = 0usize;
        let mut with_mismatched_address = 0usize;

        // TODO: Process in chunks.
        for item in reader.get_jetton_wallets_brief_info()? {
            let info = item?;
            total_wallets += 1;

            // NOTE: There are no writers to `jetton_master_accounts`.
            let Some(jetton_account) = jetton_masters.get(&info.jetton) else {
                // Unknown jetton master.
                to_remove.push(info.address);
                with_unknown_master += 1;
                continue;
            };

            let owner = SafeRc::new_dyn_value(OwnedCellSlice::new_allow_exotic(
                CellBuilder::build_from(info.owner).unwrap(),
            ));

            let Ok(output) = executor.run_getter::<GetWalletAddressOutput>(
                &jetton_account,
                RunGetterParams::new(JettonMasterInterface::get_wallet_address())
                    .with_args([owner]),
            ) else {
                // TODO: Remove master account here?
                // Getter failed.
                to_remove.push(info.address);
                with_failed_getter += 1;
                continue;
            };

            if output.address != info.address {
                // Computed address mismatch.
                to_remove.push(info.address);
                with_mismatched_address += 1;
            }
        }

        drop(reader);

        let tx = TokensRepoTransaction::default();
        tx.remove_jetton_wallets(to_remove);
        tokens.write_blocking(tx)?;

        tracing::info!(
            total_wallets,
            with_unknown_master,
            with_failed_getter,
            with_mismatched_address,
            elapsed = %humantime::format_duration(started_at.elapsed()),
            "verified jetton wallets",
        );

        Ok::<_, anyhow::Error>(tokens)
    })
    .await??;

    // Finalize state.
    tokens
        .with_transaction(|tx| {
            tx.remove_unused_interfaces();
            tx.set_reindex_complete(init_mc_block);
        })
        .await
        .context("failed to cleanup unused interfaces")?;

    // Done
    Ok(SyncOutput { cache })
}

struct InitialSyncContext<'a> {
    tokens: TokensRepo,
    parser: InterfaceParser<'a>,
    jetton_master_accounts: FastDashMap<StdAddr, Account>,
}

impl InitialSyncContext<'_> {
    fn process_batch(&self, shard_ident: ShardIdent, accounts: ShardAccountsDict) -> Result<()> {
        let Ok::<i8, _>(workchain) = shard_ident.workchain().try_into() else {
            anyhow::bail!("non-standard workchains are not supported");
        };

        let mut batch = InterfaceParserBatch::default();

        let mut total_accounts = 0usize;
        let mut total_known_interfaces = 0usize;
        let mut total_errors = 0usize;
        for item in accounts.iter() {
            let (hash, (_, state)) = item?;
            let Some(mut account) = state.load_account()? else {
                continue;
            };

            total_accounts += 1;

            let address = StdAddr::new(workchain, hash);
            match self
                .parser
                .handle_account(&address, &mut account, &mut batch)
            {
                Ok(Some(ty)) => {
                    total_known_interfaces += 1;

                    if ty == InterfaceType::JettonMaster {
                        self.jetton_master_accounts.insert(address, account);
                    }
                }
                Ok(None) => {}
                Err(_) => total_errors += 1,
            }
        }

        tracing::info!(
            %shard_ident,
            total_accounts,
            total_known_interfaces,
            total_errors,
            "processed virtual shard",
        );

        // TODO: Wait somewhere else?
        let tx = TokensRepoTransaction::default();
        tx.insert_known_interfaces(batch.new_interfaces.into_values().collect());
        tx.insert_jetton_masters(batch.jetton_masters);
        tx.insert_jetton_wallets(batch.jetton_wallets);
        let affected_rows = self
            .tokens
            .write_blocking(tx)
            .context("failed to write tokens info batch")?;
        tracing::info!(affected_rows, "inserted tokens info batch");

        Ok(())
    }
}

struct LoadedVirtualShards {
    virtual_shards: FastHashMap<ShardIdent, ShardAccountsDict>,
    libraries: Dict<HashBytes, LibDescr>,
    config: BlockchainConfigParams,
    timings: GenTimings,
}

async fn load_virtual_shards(
    core_storage: &CoreStorage,
    rpc_state: &RpcState,
    mc_block_id: &BlockId,
) -> Result<LoadedVirtualShards> {
    let mut virtual_shards = FastHashMap::default();

    let states = core_storage.shard_state_storage();
    let depth = rpc_state.config().shard_split_depth;

    let mc_state = states
        .load_state(mc_block_id)
        .await
        .context("failed to load initial mc state after boot")?;

    let timings = GenTimings {
        gen_lt: mc_state.as_ref().gen_lt,
        gen_utime: mc_state.as_ref().gen_utime,
    };

    let config;
    let shard_block_ids = {
        let extra = mc_state.state_extra()?;
        config = extra.config.params.clone();
        extra
            .shards
            .latest_blocks()
            .collect::<Result<Vec<_>, tycho_types::error::Error>>()?
    };

    let libraries = mc_state.as_ref().libraries.clone();

    let mut split_full_state = |shard_state: ShardStateStuff| {
        let (accounts, _) = shard_state.as_ref().accounts.load()?.into_parts();
        split_shard(
            &shard_state.block_id().shard,
            &accounts,
            depth,
            &mut virtual_shards,
        )
        .context("failed to split shard state into virtual shards")
    };

    for block_id in shard_block_ids {
        let sc_state = states
            .load_state(&block_id)
            .await
            .context("failed to load initial sc state after boot")?;
        split_full_state(sc_state)?;
    }
    split_full_state(mc_state)?;

    Ok(LoadedVirtualShards {
        virtual_shards,
        libraries,
        config,
        timings,
    })
}

fn split_shard(
    shard: &ShardIdent,
    accounts: &ShardAccountsDict,
    depth: u8,
    shards: &mut FastHashMap<ShardIdent, ShardAccountsDict>,
) -> Result<()> {
    fn split_shard_impl(
        shard: &ShardIdent,
        accounts: &ShardAccountsDict,
        depth: u8,
        shards: &mut FastHashMap<ShardIdent, ShardAccountsDict>,
        builder: &mut CellBuilder,
    ) -> Result<()> {
        let (left_shard_ident, right_shard_ident) = 'split: {
            if depth > 0 {
                if let Some((left, right)) = shard.split() {
                    break 'split (left, right);
                }
            }
            shards.insert(*shard, accounts.clone());
            return Ok(());
        };

        let (left_accounts, right_accounts) = {
            builder.clear_bits();
            let prefix_len = shard.prefix_len();
            if prefix_len > 0 {
                builder.store_uint(shard.prefix() >> (64 - prefix_len), prefix_len)?;
            }
            accounts.split_by_prefix(&builder.as_data_slice())?
        };

        split_shard_impl(
            &left_shard_ident,
            &left_accounts,
            depth - 1,
            shards,
            builder,
        )?;
        split_shard_impl(
            &right_shard_ident,
            &right_accounts,
            depth - 1,
            shards,
            builder,
        )
    }

    split_shard_impl(shard, accounts, depth, shards, &mut CellBuilder::new())
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;
