use anyhow::{Result, anyhow};
use num_bigint::BigInt;
use tycho_rpc::GenTimings;
use tycho_types::dict::Dict;
use tycho_types::models::{
    Account, AccountState, BlockchainConfigParams, CurrencyCollection, IntAddr, LibDescr, StdAddr,
};
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_vm::{
    BehaviourModifiers, GasParams, RcStackValue, SafeRc, SmcInfoTonV6, Stack, UnpackedConfig,
};

use crate::util::tonlib_helpers::StackParser;

// === Getter output ===

#[derive(Debug)]
pub struct GetJettonDataOutput {
    pub total_supply: num_bigint::BigUint,
    pub mintable: bool,
    pub admin_address: Option<StdAddr>,
    pub jetton_content: Cell,
    pub jetton_wallet_code: Cell,
}

impl FromStack for GetJettonDataOutput {
    fn from_stack(stack: Stack) -> Result<Self> {
        let mut parser = StackParser::begin_from_bottom(stack);
        Ok(Self {
            total_supply: parser.pop_uint()?,
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

#[derive(Debug)]
pub struct GetWalletDataOutput {
    pub balance: num_bigint::BigUint,
    pub owner: StdAddr,
    pub jetton: StdAddr,
}

impl FromStack for GetWalletDataOutput {
    fn from_stack(stack: Stack) -> Result<Self> {
        let mut parser = StackParser::begin_from_bottom(stack);
        let res = Self {
            balance: parser.pop_uint()?,
            owner: parser.pop_address()?,
            jetton: parser.pop_address()?,
        };
        parser.pop_cell()?;
        Ok(res)
    }

    fn field_count_hint() -> Option<usize> {
        Some(4)
    }
}

// === Executor ===

pub struct SimpleExecutor {
    pub libraries: Dict<HashBytes, LibDescr>,
    pub raw_config: BlockchainConfigParams,
    pub unpacked_config: UnpackedConfig,
    pub timings: GenTimings,
    pub modifiers: BehaviourModifiers,
}

impl SimpleExecutor {
    pub fn new(
        config: BlockchainConfigParams,
        libraries: Dict<HashBytes, LibDescr>,
        timings: GenTimings,
    ) -> Result<Self> {
        Ok(Self {
            libraries,
            unpacked_config: SmcInfoTonV6::unpack_config_partial(&config, timings.gen_utime)?,
            raw_config: config,
            timings,
            modifiers: Default::default(),
        })
    }

    pub fn resolve_library(&self, code: &Cell) -> Result<Cell, tycho_types::error::Error> {
        debug_assert!(code.descriptor().is_library());

        let mut cs = code.as_slice()?;
        cs.skip_first(8, 0)?;

        let lib_hash = cs.load_u256()?;
        let Some(descr) = self.libraries.get(&lib_hash)? else {
            return Err(tycho_types::error::Error::CellUnderflow);
        };

        Ok(descr.lib)
    }

    pub fn run_getter<R: FromStack>(
        &self,
        account: Account,
        params: RunGetterParams,
    ) -> Result<R, ExecutorError> {
        let VmOutput {
            exit_code, stack, ..
        } = self.run_getter_raw(account, params)?;

        // Parse output.
        if exit_code != 0 {
            return Err(ExecutorError::FailedToParse(anyhow!(
                "non-zero result code: {exit_code}"
            )));
        }

        if let Some(require_fields) = R::field_count_hint() {
            if stack.items.len() < require_fields {
                return Err(ExecutorError::FailedToParse(anyhow!(
                    "too few stack arguments"
                )));
            }
        }

        R::from_stack(stack).map_err(ExecutorError::FailedToParse)
    }

    // TODO: Use in toncenter V2 API.
    pub fn run_getter_raw(
        &self,
        account: Account,
        params: RunGetterParams,
    ) -> Result<VmOutput, ExecutorError> {
        let IntAddr::Std(address) = account.address else {
            return Err(ExecutorError::StateAccess(
                tycho_types::error::Error::InvalidTag,
            ));
        };
        let AccountState::Active(state_init) = account.state else {
            return Err(ExecutorError::AccountNotActive);
        };

        let Some(code) = state_init.code else {
            return Ok(VmOutput {
                // TODO: Verify sign.
                exit_code: -14,
                gas_used: 0,
                stack: Default::default(),
            });
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

        // Prepare output.
        let stack = std::mem::take(&mut vm.stack);
        let gas_used = vm.gas.consumed();
        drop(vm);

        Ok(VmOutput {
            exit_code,
            stack: SafeRc::unwrap_or_clone(stack),
            gas_used,
        })
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

    #[allow(unused)]
    pub fn with_args<I: IntoIterator<Item = RcStackValue>>(mut self, args: I) -> Self {
        self.args = args.into_iter().collect();
        self
    }

    #[allow(unused)]
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

impl MethodId for u64 {
    #[inline]
    fn compute_id(&self) -> i64 {
        *self as i64
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

pub struct VmOutput {
    pub exit_code: i32,
    pub stack: Stack,
    pub gas_used: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("failed to prepare account state: {0}")]
    StateAccess(#[from] tycho_types::error::Error),
    #[error("account is not active")]
    AccountNotActive,
    #[error("failed to parse output: {0}")]
    FailedToParse(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use tycho_types::models::{BlockchainConfig, StateInit};
    use tycho_vm::{OwnedCellSlice, tuple};

    use super::*;

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

        let executor = SimpleExecutor::new(default_config(), Dict::new(), GenTimings {
            gen_lt: 1_000_000,
            gen_utime: 1000,
        })?;

        let account = stub_account(code, data);

        let output = executor.run_getter::<GetJettonDataOutput>(
            account.clone(),
            RunGetterParams::new("get_jetton_data"),
        )?;
        println!("{output:#?}");

        let arg = OwnedCellSlice::new_allow_exotic(CellBuilder::build_from(STUB_ADDR).unwrap());
        let output = executor
            .run_getter::<GetWalletAddressOutput>(
                account,
                RunGetterParams::new("get_wallet_address").with_args(tuple![
                    slice arg,
                ]),
            )
            .unwrap();
        println!("{output:#?}");

        Ok(())
    }

    #[test]
    fn jetton_wallet_getter_works() -> Result<()> {
        let code = Boc::decode(include_bytes!("./test/jetton_wallet_code.boc"))?;
        let data = Boc::decode_base64(
            "te6ccgECFAEAA9EAAZFQTAW7AjgB8zYowBXIml4lYvHRjdV68RJV0Fi8i20THzvW/WQH84kAHKcE7bfLAfL8KBqtj00r0VPPTIOwbzY+1xtJNCAahfCgAQEU/wD0pBP0vPLICwICAWIDBAICywUGABug9gXaiaH0AfSB9IGpowIBzgcIAgFYCwwC9wgxwCSXwTgAdDTAwFxsJUTXwPwHeD6QPpAMfoAMXHXIfoAMfoAMHOptAAC0x8B2zxbMjQ0NCSCEA+KfqW6mjBsIjZeMRAj8BrgJIIQF41FGbqbMGwiXjIQJEMA8BvgN1s2ghBZXwe8up8CcbDy0sBQI7ry4sYB8BzgXwWAJCgARPpEMMAA8uFNgAFyAT/gzIG6VMICx+DPeIG7y0prQ0wcx0//T//QE0wfUMND6APoA+gD6APoA+gAwAAiED/LwAgFYDQ4CAUgSEwH3AXTPwEB+gD6QCHwAe1E0PoA+kD6QNTRUTahUizHBfLiwSrC//LiwlQ0QnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJIHAB+QB0yMsCEsoHy//J0AT6QPQEMfoAINdJwgDy4sTIgBgBywVQB88WcPoCdwHLa4A8C8ztRND6APpA+kDU0QrTPwEB+gBRUaAF+kD6QFNdxwVUc29wVCATVBQDyFAE+gJYzxYBzxbMySLIywES9AD0AMsAyXAB+QB0yMsCEsoHy//J0FAPxwUesfLiwwz6AFHKoSm2CBmhUAegGKEmkmxV4w0l1wsBwwAhwgCwgEBEAqhPMyIIQF41FGVgKAssfyz9QB/oCIs8WUAbPFiX6AlADzxbJUAXMI5FykXHiUAeoE6AIqgBQBKAXoBS88uLFAcmAQPsAQwDIUAT6AljPFgHPFszJ7VQAclJpoBihyIIQc2LQnCkCyx/LP1AH+gJQBM8WUAfPFsnIgBABywUnzxZQBPoCcQHLahPMyXH7AFBCEwB0jiPIgBABywVQBs8WUAX6AnABy2qCENUydttYBQLLH8s/yXL7AJJbM+JAA8hQBPoCWM8WAc8WzMntVADrO1E0PoA+kD6QNTRBdM/AQH6ACHCAPLiwvpA9AQB0NOf0QHRUWKhUljHBfLiwSbC//LiwsiCEHvdl95YBALLH8s/AfoCI88WAc8WE8ufyciAGAHLBSPPFnD6AnEBy2rMyYBA+wBAE8hQBPoCWM8WAc8WzMntVIACHIAg1yHtRND6APpA+kDU0QTTHwGEDyGCEBeNRRm6AoIQe92X3roSsfL00z8BMPoAMBOgUCPIUAT6AljPFgHPFszJ7VSA=",
        )?;

        let executor = SimpleExecutor::new(default_config(), Dict::new(), GenTimings {
            gen_lt: 1_000_000,
            gen_utime: 1000,
        })?;

        let account = stub_account(code, data);

        let output = executor
            .run_getter::<GetWalletDataOutput>(account, RunGetterParams::new("get_wallet_data"))?;
        println!("{output:#?}");

        Ok(())
    }
}
