use everscale_types::dict::RawKeys;
use everscale_types::prelude::*;
use once_cell::race::OnceBox;
use tycho_util::FastHashSet;

use crate::util::tonlib_helpers::compute_method_id;

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
) -> Result<(), everscale_types::error::Error> {
    const MAX_METHOD_ID_BITS: u16 = 64;
    const MAX_KEY_BITS: u16 = 256;

    getter_ids.clear();

    let mut cs = code.as_slice()?;

    // Parse SETCP 0
    if cs.load_u16()? != 0xff_00 {
        return Err(everscale_types::error::Error::InvalidTag);
    }

    // Parse DICTPUSHCONST
    if cs.load_uint(14)? != 0b11110100101001 {
        return Err(everscale_types::error::Error::InvalidTag);
    }

    // Load a bit length of the dictionary.
    let n = cs.load_uint(10)? as u16;
    if n >= MAX_KEY_BITS {
        return Err(everscale_types::error::Error::InvalidData);
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::util::tonlib_helpers::compute_method_id;

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
}
