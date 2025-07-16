use anyhow::{Context, Result};
use tycho_block_util::state::ShardStateStuff;
use tycho_core::storage::CoreStorage;
use tycho_rpc::{GenTimings, RpcState};
use tycho_types::models::{
    Account, AccountState, BlockId, BlockchainConfigParams, DepthBalanceInfo, LibDescr,
    ShardAccount, ShardIdent, StateInit, StdAddr,
};
use tycho_types::prelude::*;
use tycho_util::{FastDashMap, FastDashSet, FastHashMap};

use crate::state::interface::{
    GetJettonDataOutput, GetWalletDataOutput, InterfaceType, JettonMasterInterface,
    JettonWalletInterface,
};
use crate::state::models::{JettonMaster, JettonWallet, KnownInterface};
use crate::state::repo::{TokensRepo, TokensRepoTransaction};
use crate::util::tonlib_helpers::{RunGetterParams, SimpleExecutor};

// TODO: Move into config.
const BATCH_SIZE: usize = 10;

pub struct SyncOutput {
    pub known_interfaces: FastDashMap<HashBytes, InterfaceType>,
}

pub async fn sync_after_boot(
    init_mc_block: &BlockId,
    core_storage: &CoreStorage,
    rpc_state: &RpcState,
    tokens: &TokensRepo,
) -> Result<SyncOutput> {
    // Split virtual shards.
    let LoadedVirtualShards {
        virtual_shards,
        libraries,
        config,
        timings,
    } = load_virtual_shards(core_storage, rpc_state, init_mc_block)
        .await
        .context("failed to load virtual shards")?;

    // Load known code hashes cache.
    let known_interfaces = tokens
        .get_all_known_interfaces()
        .await?
        .into_iter()
        .collect::<FastDashMap<_, _>>();

    let executor = SimpleExecutor::new(config, libraries, timings)
        .context("failed to create a simple executor")?;

    let tokens = tokens.clone();
    let context = tokio::task::spawn_blocking(move || {
        let context = InitialSyncContext {
            known_interfaces,
            skip_code: Default::default(),
            tokens,
            executor,
        };

        // TODO: Optimize.
        let mut groups = (0..BATCH_SIZE)
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
                        if let Err(e) = context.run(shard_ident, accounts) {
                            // TODO: Should we just unwrap here?
                            tracing::error!("FATAL, failed to process virtual shard: {e:?}");
                        }
                    }
                });
            }
        });

        context
    })
    .await?;

    // Done
    Ok(SyncOutput {
        known_interfaces: context.known_interfaces,
    })
}

struct InitialSyncContext {
    known_interfaces: FastDashMap<HashBytes, InterfaceType>,
    skip_code: FastDashSet<HashBytes>,
    tokens: TokensRepo,
    executor: SimpleExecutor,
}

#[derive(Default)]
struct BatchResult {
    new_interfaces: FastHashMap<HashBytes, KnownInterface>,
    jetton_masters: Vec<JettonMaster>,
    jetton_wallets: Vec<JettonWallet>,
}

impl InitialSyncContext {
    fn run(&self, shard_ident: ShardIdent, accounts: ShardAccountsDict) -> Result<()> {
        let Ok::<i8, _>(workchain) = shard_ident.workchain().try_into() else {
            anyhow::bail!("non-standard workchains are not supported");
        };

        let mut batch = BatchResult::default();

        let mut total_accounts = 0usize;
        let mut total_known_interfaces = 0usize;
        let mut total_errors = 0usize;
        for item in accounts.iter() {
            let (hash, (_, state)) = item?;
            let Some(account) = state.load_account()? else {
                continue;
            };

            total_accounts += 1;

            let address = StdAddr::new(workchain, hash);
            match self.handle_account(&address, account, &mut batch) {
                Ok(known) => total_known_interfaces += known as usize,
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

    fn handle_account(
        &self,
        address: &StdAddr,
        mut account: Account,
        batch: &mut BatchResult,
    ) -> Result<bool> {
        let AccountState::Active(StateInit {
            code: Some(code),
            data,
            ..
        }) = &mut account.state
        else {
            return Ok(false);
        };

        if code.descriptor().is_library() {
            *code = self.executor.resolve_library(code)?;
        }

        let code_hash = *code.repr_hash();
        let data_hash = data.as_ref().map(|x| *x.repr_hash()).unwrap_or_default();

        let known_interface = self.known_interfaces.get(&code_hash).map(|item| *item);
        let known_interface = if let Some(interface) = known_interface {
            interface
        } else if self.skip_code.contains(&code_hash) {
            return Ok(false);
        } else if let Some(interface) = InterfaceType::detect(code.as_ref()) {
            interface
        } else {
            self.skip_code.insert(code_hash);
            return Ok(false);
        };

        account.address = address.clone().into();

        let res = match known_interface {
            InterfaceType::JettonMaster => {
                self.handle_jetton_master(address, &code_hash, &data_hash, account, batch)
            }
            InterfaceType::JettonWallet => {
                self.handle_jetton_wallet(address, &code_hash, &data_hash, account, batch)
            }
        };

        if res.is_ok() {
            self.known_interfaces.insert(code_hash, known_interface);
            batch.new_interfaces.insert(code_hash, KnownInterface {
                code_hash,
                interface: known_interface as u8,
                is_broken: false,
            });
        } else {
            self.skip_code.insert(code_hash);
        }
        Ok(true)
    }

    fn handle_jetton_master(
        &self,
        address: &StdAddr,
        code_hash: &HashBytes,
        data_hash: &HashBytes,
        account: Account,
        batch: &mut BatchResult,
    ) -> Result<()> {
        let last_transaction_lt = account.last_trans_lt;
        let output = self.executor.run_getter::<GetJettonDataOutput>(
            account,
            RunGetterParams::new(JettonMasterInterface::get_jetton_data()),
        )?;

        batch.jetton_masters.push(JettonMaster {
            address: address.clone(),
            total_supply: output.total_supply,
            mintable: output.mintable,
            admin_address: output.admin_address,
            // TODO: Extract jetton content
            jetton_content: None,
            wallet_code_hash: *output.jetton_wallet_code.repr_hash(),
            last_transaction_lt,
            code_hash: *code_hash,
            data_hash: *data_hash,
        });

        Ok(())
    }

    fn handle_jetton_wallet(
        &self,
        address: &StdAddr,
        code_hash: &HashBytes,
        data_hash: &HashBytes,
        account: Account,
        batch: &mut BatchResult,
    ) -> Result<()> {
        let last_transaction_lt = account.last_trans_lt;
        let output = self.executor.run_getter::<GetWalletDataOutput>(
            account,
            RunGetterParams::new(JettonWalletInterface::get_wallet_data()),
        )?;

        batch.jetton_wallets.push(JettonWallet {
            address: address.clone(),
            balance: output.balance,
            owner: output.owner,
            jetton: output.jetton,
            last_transaction_lt,
            code_hash: Some(*code_hash),
            data_hash: Some(*data_hash),
        });

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
