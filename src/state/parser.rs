use anyhow::Result;
use once_cell::race::OnceBox;
use tycho_types::dict::RawKeys;
use tycho_types::models::{Account, AccountState, StateInit, StdAddr};
use tycho_types::prelude::*;
use tycho_util::{FastDashMap, FastDashSet, FastHashMap, FastHashSet};
use tycho_vm::Stack;

use super::models::{JettonMaster, JettonWallet, KnownInterface};
use crate::util::tonlib_helpers::{
    FromStack, RunGetterParams, SimpleExecutor, StackParser, compute_method_id,
};

// === Parser ===

#[derive(Default)]
pub struct InterfaceCache {
    pub known_interfaces: FastDashMap<HashBytes, InterfaceType>,
    // TODO: Use moka with some limits.
    pub skip_code: FastDashSet<HashBytes>,
}

impl InterfaceCache {
    pub fn merge(&self, other: InterfaceCache) {
        // TODO: Optimize.
        for (code_hash, interface) in other.known_interfaces {
            self.known_interfaces.insert(code_hash, interface);
        }

        // TODO: Optimize.
        for code_hash in other.skip_code {
            self.skip_code.insert(code_hash);
        }
    }
}

#[derive(Default)]
pub struct InterfaceParserBatch {
    pub new_interfaces: FastHashMap<HashBytes, KnownInterface>,
    pub jetton_masters: Vec<JettonMaster>,
    pub jetton_wallets: Vec<JettonWallet>,
}

pub struct InterfaceParser<'a> {
    pub cache: &'a InterfaceCache,
    pub executor: SimpleExecutor,
}

impl InterfaceParser<'_> {
    pub fn handle_account(
        &self,
        address: &StdAddr,
        mut account: Account,
        batch: &mut InterfaceParserBatch,
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

        let cache = self.cache;

        let known_interface = cache.known_interfaces.get(&code_hash).map(|item| *item);
        let known_interface = if let Some(interface) = known_interface {
            interface
        } else if cache.skip_code.contains(&code_hash) {
            return Ok(false);
        } else if let Some(interface) = InterfaceType::detect(code.as_ref()) {
            interface
        } else {
            cache.skip_code.insert(code_hash);
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
            cache.known_interfaces.insert(code_hash, known_interface);
            batch.new_interfaces.insert(code_hash, KnownInterface {
                code_hash,
                interface: known_interface as u8,
                is_broken: false,
            });
        } else {
            cache.skip_code.insert(code_hash);
        }
        Ok(true)
    }

    fn handle_jetton_master(
        &self,
        address: &StdAddr,
        code_hash: &HashBytes,
        data_hash: &HashBytes,
        account: Account,
        batch: &mut InterfaceParserBatch,
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
        batch: &mut InterfaceParserBatch,
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

// === Interface Type ===

// TODO: Generate with macros?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum InterfaceType {
    JettonWallet = 0,
    JettonMaster = 1,
}

impl InterfaceType {
    pub fn from_id(id: u8) -> Option<Self> {
        Some(match id {
            0 => Self::JettonWallet,
            1 => Self::JettonMaster,
            _ => return None,
        })
    }

    pub fn detect(code: &DynCell) -> Option<Self> {
        thread_local! {
            static GETTER_IDS: std::cell::RefCell<FastHashSet<u64>> = Default::default();
        }

        GETTER_IDS.with_borrow_mut(|getter_ids| {
            parse_contract_getters(code, getter_ids).ok()?;
            if JettonWalletInterface::check_support(getter_ids) {
                Some(Self::JettonWallet)
            } else if JettonMasterInterface::check_support(getter_ids) {
                Some(Self::JettonMaster)
            } else {
                None
            }
        })
    }
}

macro_rules! define_contract_interface {
    ($ident:ident { $($name:ident),*$(,)? }) => {
        pub struct $ident;

        impl $ident {
            const METHOD_COUNT: usize = const {
                define_contract_interface!(@count { 0 } $($name)*)
            };

            $(pub fn $name() -> u64 {
                static ID: OnceBox<u64> = OnceBox::new();
                *ID.get_or_init(|| Box::new(compute_method_id(stringify!($name)) as u64))
            })*

            fn check_support(getter_ids: &FastHashSet<u64>) -> bool {
                static IDS: OnceBox<[u64; $ident::METHOD_COUNT]> = OnceBox::new();
                let ids = IDS.get_or_init(|| {
                    Box::new([$(Self::$name()),*])
                });
                for id in ids {
                    if !getter_ids.contains(&id) {
                        return false;
                    }
                }
                true
            }
        }
    };

    (@count { $expr:expr }) => { $expr };
    (@count { $expr:expr } $name:ident $($rest:ident)*) => {
        define_contract_interface!(@count { $expr + 1 } $($rest)*)
    }
}

define_contract_interface!(JettonWalletInterface { get_wallet_data });

define_contract_interface!(JettonMasterInterface {
    get_jetton_data,
    get_wallet_address
});

// TODO: Add support for Solidity ABI
fn parse_contract_getters(
    code: &DynCell,
    getter_ids: &mut FastHashSet<u64>,
) -> Result<(), tycho_types::error::Error> {
    const MAX_METHOD_ID_BITS: u16 = 64;
    const MAX_KEY_BITS: u16 = 256;

    getter_ids.clear();

    let mut cs = code.as_slice()?;

    // Parse SETCP 0
    if cs.load_u16()? != 0xff_00 {
        return Err(tycho_types::error::Error::InvalidTag);
    }

    // Parse DICTPUSHCONST
    if cs.load_uint(14)? != 0b11110100101001 {
        return Err(tycho_types::error::Error::InvalidTag);
    }

    // Load a bit length of the dictionary.
    let n = cs.load_uint(10)? as u16;
    if n >= MAX_KEY_BITS {
        return Err(tycho_types::error::Error::InvalidData);
    }

    // And parse all dictionary keys.
    let to_cut = n.checked_sub(MAX_METHOD_ID_BITS).filter(|x| *x != 0);
    let dict = Some(cs.load_reference_cloned()?);
    for key in RawKeys::new(&dict, n) {
        let key = key?;
        let mut key = key.as_data_slice();
        if let Some(to_cut) = to_cut {
            key.skip_first(to_cut, 0)?;
        }

        let method_id = key.load_uint(std::cmp::min(n, MAX_METHOD_ID_BITS))?;

        // TODO: Should we only collect methods with `0x10000` flag?
        getter_ids.insert(method_id);
    }

    // Done
    Ok(())
}

// === Getter outputs ===

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

#[cfg(test)]
mod test {
    use tycho_rpc::GenTimings;
    use tycho_types::models::{
        Account, AccountState, BlockchainConfig, BlockchainConfigParams, CurrencyCollection,
        StateInit,
    };
    use tycho_vm::{OwnedCellSlice, tuple};

    use super::*;
    use crate::util::tonlib_helpers::{RunGetterParams, SimpleExecutor, compute_method_id};

    const STUB_ADDR: StdAddr = StdAddr::new(0, HashBytes::ZERO);
    const STUB_BALANCE: CurrencyCollection = CurrencyCollection::new(1_000_000_000);

    #[test]
    fn parses_jetton_master() {
        let code = Boc::decode(include_bytes!("./test/jetton_master_code.boc")).unwrap();
        let mut getter_ids = FastHashSet::default();
        parse_contract_getters(code.as_ref(), &mut getter_ids).unwrap();

        for name in ["get_jetton_data", "get_wallet_address"] {
            let target_method_id = compute_method_id(name) as u64;
            assert!(getter_ids.contains(&target_method_id));
        }

        assert_eq!(
            InterfaceType::detect(code.as_ref()),
            Some(InterfaceType::JettonMaster)
        );
    }

    #[test]
    fn parses_jetton_wallet() {
        let code = Boc::decode(include_bytes!("./test/jetton_wallet_code.boc")).unwrap();
        let mut getter_ids = FastHashSet::default();
        parse_contract_getters(code.as_ref(), &mut getter_ids).unwrap();

        let target_method_id = compute_method_id("get_wallet_data") as u64;
        assert!(getter_ids.contains(&target_method_id));

        assert_eq!(
            InterfaceType::detect(code.as_ref()),
            Some(InterfaceType::JettonWallet)
        );
    }

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
