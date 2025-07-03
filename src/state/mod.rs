use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_core::storage::CoreStorage;
use tycho_rpc::{GenTimings, RpcState};
use tycho_storage::StorageContext;
use tycho_types::cell::{CellBuilder, HashBytes};
use tycho_types::dict::Dict;
use tycho_types::models::{
    Account, AccountState, BlockId, BlockchainConfigParams, DepthBalanceInfo, LibDescr,
    ShardAccount, ShardIdent, StateInit, StdAddr,
};
use tycho_util::{FastDashMap, FastDashSet, FastHashMap};
use tycho_vm::UnpackedConfig;

use self::interface::{InterfaceType, JettonMasterInterface, JettonWalletInterface};
use self::models::KnownInterface;
use self::parser::{GetJettonDataOutput, GetWalletDataOutput, RunGetterParams, SimpleExecutor};
pub use self::repo::TokensRepo;

pub mod interface;
pub mod models;
pub mod util;

mod db;
mod parser;
mod repo;

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
        // TODO: Move into config.
        const BATCH_SIZE: usize = 10;

        let tokens = self.tokens();

        // Split virtual shards.
        let LoadedVirtualShards {
            virtual_shards,
            libraries,
            config,
            timings,
        } = self
            .load_virtual_shards(init_mc_block)
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

        let shared = self.inner.clone();
        let context = tokio::task::spawn_blocking(move || {
            let context = InitialSyncContext {
                known_interfaces,
                skip_interfaces: Default::default(),
                new_interfaces: Default::default(),
                shared,
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

        // Save new interfaces an fill up the cache.
        let new_interfaces = context
            .new_interfaces
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();

        let updated_rows = tokens
            .insert_known_interfaces(new_interfaces)
            .await
            .context("failed to save new interfaces")?;
        tracing::info!(updated_rows, "saved new known interfaces");

        // TODO: Optimize.
        for (code_hash, interface) in context.known_interfaces.into_iter() {
            self.inner.known_interfaces.insert(code_hash, interface);
        }

        // Done
        Ok(())
    }

    async fn load_virtual_shards(&self, mc_block_id: &BlockId) -> Result<LoadedVirtualShards> {
        let mut virtual_shards = FastHashMap::default();

        let states = self.inner.core_storage.shard_state_storage();
        let depth = self.rpc_state().config().shard_split_depth;

        let mc_state = states
            .load_state(&mc_block_id)
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
}

impl axum::extract::FromRef<TonCenterRpcState> for RpcState {
    #[inline]
    fn from_ref(input: &TonCenterRpcState) -> Self {
        input.inner.rpc_state.clone()
    }
}

struct LoadedVirtualShards {
    virtual_shards: FastHashMap<ShardIdent, ShardAccountsDict>,
    libraries: Dict<HashBytes, LibDescr>,
    config: BlockchainConfigParams,
    timings: GenTimings,
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

// === Sync after boot ===

struct InitialSyncContext {
    known_interfaces: FastDashMap<HashBytes, InterfaceType>,
    skip_interfaces: FastDashSet<HashBytes>,
    new_interfaces: FastDashMap<HashBytes, KnownInterface>,
    shared: Arc<Inner>,
    executor: SimpleExecutor,
}

impl InitialSyncContext {
    fn run(&self, shard_ident: ShardIdent, accounts: ShardAccountsDict) -> Result<()> {
        let Ok::<i8, _>(workchain) = shard_ident.workchain().try_into() else {
            anyhow::bail!("non-standard workchains are not supported");
        };

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
            match self.handle_account(&address, account) {
                Ok(_) => {}
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
        Ok(())
    }

    fn handle_account(&self, address: &StdAddr, mut account: Account) -> Result<bool> {
        let AccountState::Active(StateInit {
            code: Some(code), ..
        }) = &mut account.state
        else {
            return Ok(false);
        };

        if code.descriptor().is_library() {
            *code = self.executor.resolve_library(code)?;
        }

        let code_hash = *code.repr_hash();

        let known_interface = self.known_interfaces.get(&code_hash).map(|item| *item);

        let known_interface = if let Some(interface) = known_interface {
            interface
        } else if self.skip_interfaces.contains(&code_hash) {
            return Ok(false);
        } else if let Some(interface) = InterfaceType::detect(code.as_ref()) {
            interface
        } else {
            self.skip_interfaces.insert(code_hash);
            return Ok(false);
        };

        account.address = address.clone().into();

        match known_interface {
            InterfaceType::JettonMaster => {
                let Ok(_output) = self.executor.run_getter::<GetJettonDataOutput>(
                    account,
                    RunGetterParams::new(JettonMasterInterface::get_jetton_data()),
                ) else {
                    return Ok(false);
                };

                self.known_interfaces
                    .insert(code_hash, InterfaceType::JettonMaster);
            }
            InterfaceType::JettonWallet => {
                let Ok(_output) = self.executor.run_getter::<GetWalletDataOutput>(
                    account,
                    RunGetterParams::new(JettonWalletInterface::get_wallet_data()),
                ) else {
                    return Ok(false);
                };

                self.known_interfaces
                    .insert(code_hash, InterfaceType::JettonWallet);
            }
        }

        Ok(true)
    }
}

// === Helpers ===

// TODO: Move into common utils.
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
