use anyhow::{Result, anyhow};
use everscale_types::dict::Dict;
use everscale_types::models::{
    Account, AccountState, BlockchainConfigParams, CurrencyCollection, IntAddr, LibDescr,
    ShardAccount, ShardAccounts, StdAddr,
};
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use num_bigint::BigInt;
use tycho_rpc::GenTimings;
use tycho_vm::{
    BehaviourModifiers, GasParams, RcStackValue, SafeRc, SmcInfoTonV6, Stack, UnpackedConfig,
};

use crate::util::tonlib_helpers::StackParser;

// === Getter output ===

#[derive(Debug)]
pub struct GetJettonDataOutput {
    pub total_supply: num_bigint::BigInt,
    pub mintable: bool,
    pub admin_address: Option<StdAddr>,
    pub jetton_content: Cell,
    pub jetton_wallet_code: Cell,
}

impl FromStack for GetJettonDataOutput {
    fn from_stack(stack: Stack) -> Result<Self> {
        let mut parser = StackParser::begin_from_bottom(stack);
        Ok(Self {
            total_supply: parser.pop_int()?,
            mintable: parser.pop_bool()?,
            admin_address: parser.pop_address_or_none()?,
            jetton_content: parser.pop_cell()?,
            jetton_wallet_code: parser.pop_cell()?,
        })
    }

    fn field_count_hint() -> Option<usize> {
        Some(5)
    }
}

#[derive(Debug)]
pub struct GetWalletAddressOutput {
    pub address: StdAddr,
}

impl FromStack for GetWalletAddressOutput {
    fn from_stack(stack: Stack) -> Result<Self> {
        let mut parser = StackParser::begin_from_bottom(stack);
        Ok(Self {
            address: parser.pop_address()?,
        })
    }

    fn field_count_hint() -> Option<usize> {
        Some(1)
    }
}

// === Executor ===

pub struct SimpleExecutor<P> {
    pub state_provider: P,
    pub libraries: Dict<HashBytes, LibDescr>,
    pub raw_config: BlockchainConfigParams,
    pub unpacked_config: UnpackedConfig,
    pub timings: GenTimings,
    pub modifiers: BehaviourModifiers,
}

impl<P> SimpleExecutor<P> {
    pub fn new(
        state_provider: P,
        libraries: Dict<HashBytes, LibDescr>,
        raw_config: BlockchainConfigParams,
        timings: GenTimings,
    ) -> Result<Self> {
        let unpacked_config = SmcInfoTonV6::unpack_config_partial(&raw_config, timings.gen_utime)?;

        Ok(Self {
            state_provider,
            libraries,
            raw_config,
            unpacked_config,
            timings,
            modifiers: Default::default(),
        })
    }
}

impl<P: StateProvider> SimpleExecutor<P> {
    pub fn run_getter<R: FromStack>(
        &self,
        address: &StdAddr,
        params: RunGetterParams,
    ) -> Result<R, ExecutorError> {
        // Find account state in shard accounts dictionary.
        let Some(account) = self.state_provider.get_state(address)? else {
            return Err(ExecutorError::NotFound);
        };

        let AccountState::Active(state_init) = account.state else {
            return Err(ExecutorError::AccountNotActive);
        };

        let code = match state_init.code {
            // Resolve library cells.
            Some(code) if code.descriptor().is_library() => self.resolve_library(code)?,
            // Otherwise just use the code if any.
            Some(code) => code,
            // Or return a fatal error.
            None => return Err(ExecutorError::Exception(14)),
        };

        // Prepare VM state.
        let (gas, stack) = params.build();

        let smc_info = tycho_vm::SmcInfoBase::new()
            .with_now(self.timings.gen_utime)
            .with_block_lt(self.timings.gen_lt)
            .with_tx_lt(self.timings.gen_lt)
            .with_account_balance(account.balance)
            .with_account_addr(address.clone().into())
            .with_config(self.raw_config.clone())
            .require_ton_v4()
            .with_code(code.clone())
            .with_message_balance(CurrencyCollection::ZERO)
            .with_storage_fees(Tokens::ZERO)
            .require_ton_v6()
            .with_unpacked_config(self.unpacked_config.as_tuple())
            .require_ton_v11();

        let libraries = (state_init.libraries, &self.libraries);
        let mut vm = tycho_vm::VmState::builder()
            .with_smc_info(smc_info)
            .with_code(code)
            .with_data(state_init.data.unwrap_or_default())
            .with_libraries(&libraries)
            .with_init_selector(false)
            .with_raw_stack(stack)
            .with_gas(gas)
            .with_modifiers(self.modifiers)
            .build();

        // Run VM.
        let exit_code = !vm.run();

        // Parse output.
        if exit_code != 0 {
            return Err(ExecutorError::Exception(exit_code));
        }
        if let Some(require_fields) = R::field_count_hint() {
            if vm.stack.items.len() < require_fields {
                return Err(ExecutorError::FailedToParse(anyhow!(
                    "too few stack arguments"
                )));
            }
        }

        let stack = std::mem::take(&mut vm.stack);
        drop(vm);

        R::from_stack(SafeRc::unwrap_or_clone(stack)).map_err(ExecutorError::FailedToParse)
    }

    fn resolve_library(&self, code: Cell) -> Result<Cell, everscale_types::error::Error> {
        debug_assert!(code.descriptor().is_library());

        let mut cs = code.as_slice()?;
        cs.skip_first(8, 0)?;

        let lib_hash = cs.load_u256()?;
        let Some(descr) = self.libraries.get(&lib_hash)? else {
            return Err(everscale_types::error::Error::CellUnderflow);
        };

        Ok(descr.lib)
    }
}

pub trait StateProvider {
    fn get_state(
        &self,
        address: &StdAddr,
    ) -> Result<Option<Account>, everscale_types::error::Error>;
}

pub struct ShardStateProvider {
    pub workchain: i32,
    pub accounts: ShardAccounts,
}

impl StateProvider for ShardStateProvider {
    fn get_state(
        &self,
        address: &StdAddr,
    ) -> Result<Option<Account>, everscale_types::error::Error> {
        if self.workchain != address.workchain as i32 {
            return Ok(None);
        }
        let Some((_, state)) = self.accounts.get(&address.address)? else {
            return Ok(None);
        };
        Ok(state.load_account()?)
    }
}

impl StateProvider for Account {
    fn get_state(
        &self,
        account: &StdAddr,
    ) -> Result<Option<Account>, everscale_types::error::Error> {
        Ok(
            if matches!(&self.address, IntAddr::Std(addr) if addr == account) {
                Some(self.clone())
            } else {
                None
            },
        )
    }
}

pub struct RunGetterParams {
    pub method_id: i64,
    pub gas_limit: u64,
    pub args: Vec<RcStackValue>,
}

impl RunGetterParams {
    const DEFAULT_GAS_LIMIT: u64 = 200_000;

    pub fn new<T: MethodId>(id: T) -> Self {
        Self {
            method_id: id.compute_id(),
            gas_limit: Self::DEFAULT_GAS_LIMIT,
            args: Vec::new(),
        }
    }

    pub fn with_args<I: IntoIterator<Item = RcStackValue>>(mut self, args: I) -> Self {
        self.args = args.into_iter().collect();
        self
    }

    pub fn with_gas_limit(mut self, limit: u64) -> Self {
        self.gas_limit = limit;
        self
    }

    pub fn build(mut self) -> (GasParams, SafeRc<Stack>) {
        let gas = GasParams {
            max: self.gas_limit,
            limit: self.gas_limit,
            ..GasParams::getter()
        };

        let method_id = RcStackValue::new_dyn_value(BigInt::from(self.method_id));
        self.args.push(method_id);
        let stack = SafeRc::new(Stack::with_items(self.args));

        (gas, stack)
    }
}

pub trait MethodId {
    fn compute_id(&self) -> i64;
}

impl MethodId for i64 {
    #[inline]
    fn compute_id(&self) -> i64 {
        *self
    }
}

impl MethodId for &str {
    #[inline]
    fn compute_id(&self) -> i64 {
        crate::util::tonlib_helpers::compute_method_id(self)
    }
}

impl MethodId for String {
    #[inline]
    fn compute_id(&self) -> i64 {
        self.as_str().compute_id()
    }
}

pub trait FromStack: Sized {
    fn from_stack(stack: Stack) -> Result<Self>;

    fn field_count_hint() -> Option<usize>;
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("failed to prepare account state: {0}")]
    StateAccess(#[from] everscale_types::error::Error),
    #[error("account state not found")]
    NotFound,
    #[error("account is not active")]
    AccountNotActive,
    #[error("non-zero result code: {0}")]
    Exception(i32),
    #[error("failed to parse output: {0}")]
    FailedToParse(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use everscale_types::models::{BlockchainConfig, StateInit};
    use tycho_vm::{OwnedCellSlice, tuple};

    use super::*;
    use crate::util::tonlib_helpers::load_bytes_rope;

    const STUB_ADDR: StdAddr = StdAddr::new(0, HashBytes::ZERO);
    const STUB_BALANCE: CurrencyCollection = CurrencyCollection::new(1_000_000_000);

    fn stub_account(code: Cell, data: Cell) -> Account {
        Account {
            address: STUB_ADDR.into(),
            storage_stat: Default::default(),
            last_trans_lt: 0,
            balance: STUB_BALANCE,
            state: AccountState::Active(StateInit {
                code: Some(code),
                data: Some(data),
                ..Default::default()
            }),
        }
    }

    fn default_config() -> BlockchainConfigParams {
        let mut config: BlockchainConfig =
            BocRepr::decode(include_bytes!("./test/config.boc")).unwrap();
        config.params.set_global_id(100).unwrap();
        config.params
    }

    #[test]
    fn jetton_master_getter_works() -> Result<()> {
        let code = Boc::decode(include_bytes!("./test/jetton_master_code.boc"))?;
        let data = Boc::decode_base64(
            "te6cckECFQEAA6oAAgtcugyJMBgBAgAyAAAAAdrBf5WNLuUjoiBiBplFl8E9gx7HBgEU/wD0pBP0vPLICwMCAWIEBQICywYHABug9gXaiaH0AfSB9IGpowIBzggJAgFYDA0C9wgxwCSXwTgAdDTAwFxsJUTXwPwHeD6QPpAMfoAMXHXIfoAMfoAMHOptAAC0x8B2zxbMjQ0NCSCEA+KfqW6mjBsIjZeMRAj8BrgJIIQF41FGbqbMGwiXjIQJEMA8BvgN1s2ghBZXwe8up8CcbDy0sBQI7ry4sYB8BzgXwWAKCwARPpEMMAA8uFNgAFyAT/gzIG6VMICx+DPeIG7y0prQ0wcx0//T//QE0wfUMND6APoA+gD6APoA+gAwAAiED/LwAgFYDg8CAUgTFAH3AXTPwEB+gD6QCHwAe1E0PoA+kD6QNTRUTahUizHBfLiwSrC//LiwlQ0QnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJIHAB+QB0yMsCEsoHy//J0AT6QPQEMfoAINdJwgDy4sTIgBgBywVQB88WcPoCdwHLa4BAC8ztRND6APpA+kDU0QrTPwEB+gBRUaAF+kD6QFNdxwVUc29wVCATVBQDyFAE+gJYzxYBzxbMySLIywES9AD0AMsAyXAB+QB0yMsCEsoHy//J0FAPxwUesfLiwwz6AFHKoSm2CBmhUAegGKEmkmxV4w0l1wsBwwAhwgCwgERIAqhPMyIIQF41FGVgKAssfyz9QB/oCIs8WUAbPFiX6AlADzxbJUAXMI5FykXHiUAeoE6AIqgBQBKAXoBS88uLFAcmAQPsAQwDIUAT6AljPFgHPFszJ7VQAclJpoBihyIIQc2LQnCkCyx/LP1AH+gJQBM8WUAfPFsnIgBABywUnzxZQBPoCcQHLahPMyXH7AFBCEwB0jiPIgBABywVQBs8WUAX6AnABy2qCENUydttYBQLLH8s/yXL7AJJbM+JAA8hQBPoCWM8WAc8WzMntVADrO1E0PoA+kD6QNTRBdM/AQH6ACHCAPLiwvpA9AQB0NOf0QHRUWKhUljHBfLiwSbC//LiwsiCEHvdl95YBALLH8s/AfoCI88WAc8WE8ufyciAGAHLBSPPFnD6AnEBy2rMyYBA+wBAE8hQBPoCWM8WAc8WzMntVIACHIAg1yHtRND6APpA+kDU0QTTHwGEDyGCEBeNRRm6AoIQe92X3roSsfL00z8BMPoAMBOgUCPIUAT6AljPFgHPFszJ7VSBn+5gZ",
        )?;

        let executor = SimpleExecutor::new(
            stub_account(code, data),
            Dict::new(),
            default_config(),
            GenTimings {
                gen_lt: 1_000_000,
                gen_utime: 1000,
            },
        )?;

        let output = executor
            .run_getter::<GetJettonDataOutput>(&STUB_ADDR, RunGetterParams::new("get_jetton_data"))
            .unwrap();
        println!("{output:#?}");

        let arg = OwnedCellSlice::new_allow_exotic(CellBuilder::build_from(STUB_ADDR).unwrap());
        let output = executor
            .run_getter::<GetWalletAddressOutput>(
                &STUB_ADDR,
                RunGetterParams::new("get_wallet_address").with_args(tuple![
                    slice arg,
                ]),
            )
            .unwrap();
        println!("{output:#?}");

        Ok(())
    }
}
