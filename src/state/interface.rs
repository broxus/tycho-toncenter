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
        let code = Boc::decode_base64("te6ccgECFgEABIwAART/APSkE/S88sgLAQIBYgIDAgLLBAUCAWoQEQS30IMcAkl8D4AHQ0wMBcbCSXwPg+kD6QDH6ADFx1yH6ADH6ADBzqbQAItMf0z9Z7UTQ+gDU1NHbPFB4Xwcq+kQpwBXjAlsnghB73ZfeuuMCMDFsIjcCghAsdrlzuoGBwgJAgEgDg8AXIBP+DMgbpUwgLH4M94gbvLSmtDTBzHT/9P/9ATTB9Qw0PoA+gD6APoA+gD6ADAB5jM4OTk5OQPA/1Fnuhaw8uGTAvpA+CdvEFAFoQT6APoA0XDIghAXjUUZWAcCyx/LPyL6AnDIywHJ0M8Wf1AJdMjLAhLKB8v/ydAYzxZQB/oCE8sAyVRGRPAcAqBmA8hQA/oCzMzJ7VSCEMBmDM/IWPoCyXAKAf4xNTU2OAP6APpAMPgoJnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQUAbHBfLhlFAkoVRgZMhQA/oCzMzJ7VTIUATPFhTMyfgnbxBYociAEAHLBVADf3RQA8sCEsoHy/9Y+gJxActqzMlwCwEQ4wJfB4QP8vAMAEiDB3GADMjLA8sBywgTy/8ClXFYy2HMmHBYy2EB0M8W4slw+wAAZPsAghDAdwzPyFj6AslwgwdxgAzIywPLAcsIE8v/ApVxWMthzJhwWMthAdDPFuLJcPsAAfxQNaAVvPLgSwP6QNMA0ZXIIc8WyZFt4siAGAHLBVADzxZw+gJwActqghDRc1QAWAQCyx/LPyL6RDDAAI42+ChDBHBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQEs8WlmwicAHLAeL0AMkNAAiAQPsAAGe7kODeARwuoodSGEGEEyVMryVMYcQq3xgDSEmAACXMZmZFOEVKpEDfAgOWDgVKBcjYQ5OhAJm58FCIBuCoQCaoKAeQoAn0BLGeLAOeLZmSRZGWAiXoAegBlgGSQOAD8gDpkZYEJZQPl/+ToZEAMAOWCgOeLKAH9ATuA5bWJZmZkuH2AQIBWBITACm0aP2omh9AGpqaJgY6GmP6c/pg+jAAfa289qJofQBqami2EPwUALgqEAmqCgHkKAJ9ASxniwDni2ZkkWRlgIl6AHoAZYBkuAD8gDpkZYEJZQPl/+ToQAHhrxb2omh9AGpqaLaBaGmP6c/pg+i9eAqBPXgKgMAIeArkRoOtDo6ODmdF5exOTSyM7KXOje3Fze5M5e6N7WytxfBniyxni0WZeYPEZ4sA54sQRalzU5t7dGeLZOgAxaFzg3M8Z4tk6AC4ZGWDgOeLZMAUAcSC8HDl17aimzkvhQdv4Vyi8gU8VsIzhyjE4zyejdse6CfMWAWDB/QXA3DIywcBzxbJgvBhBdbMdq9AAyXpTViM5RG+W/27c7Q33FHspDkX16Q+PVgEgwf0FwJwyMsHAc8WyRUAcILw7oD9Lx4DSA4igjY1lu51LXuyf1B3a5UIagJ5GJZ1kj5YA4MH9BdwyMsH9ADJf3DIywHJ0EAD").unwrap();
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
        let code = Boc::decode_base64("te6ccgECEwEAA4UAART/APSkE/S88sgLAQIBYgIDAgLLBAUAG6D2BdqJofQB9IH0gamjAgHOBgcCAVgKCwL3CDHAJJfBOAB0NMDAXGwlRNfA/Ad4PpA+kAx+gAxcdch+gAx+gAwc6m0AALTHwHbPFsyNDQ0JIIQD4p+pbqaMGwiNl4xECPwGuAkghAXjUUZupswbCJeMhAkQwDwG+A3WzaCEFlfB7y6nwJxsPLSwFAjuvLixgHwHOBfBYAgJABE+kQwwADy4U2AAXIBP+DMgbpUwgLH4M94gbvLSmtDTBzHT/9P/9ATTB9Qw0PoA+gD6APoA+gD6ADAACIQP8vACAVgMDQIBSBESAfcBdM/AQH6APpAIfAB7UTQ+gD6QPpA1NFRNqFSLMcF8uLBKsL/8uLCVDRCcFQgE1QUA8hQBPoCWM8WAc8WzMkiyMsBEvQA9ADLAMkgcAH5AHTIywISygfL/8nQBPpA9AQx+gAg10nCAPLixMiAGAHLBVAHzxZw+gJ3ActrgDgLzO1E0PoA+kD6QNTRCtM/AQH6AFFRoAX6QPpAU13HBVRzb3BUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQUA/HBR6x8uLDDPoAUcqhKbYIGaFQB6AYoSaSbFXjDSXXCwHDACHCALCAPEACqE8zIghAXjUUZWAoCyx/LP1AH+gIizxZQBs8WJfoCUAPPFslQBcwjkXKRceJQB6gToAiqAFAEoBegFLzy4sUByYBA+wBDAMhQBPoCWM8WAc8WzMntVAByUmmgGKHIghBzYtCcKQLLH8s/UAf6AlAEzxZQB88WyciAEAHLBSfPFlAE+gJxActqE8zJcfsAUEITAHSOI8iAEAHLBVAGzxZQBfoCcAHLaoIQ1TJ221gFAssfyz/JcvsAklsz4kADyFAE+gJYzxYBzxbMye1UAOs7UTQ+gD6QPpA1NEF0z8BAfoAIcIA8uLC+kD0BAHQ05/RAdFRYqFSWMcF8uLBJsL/8uLCyIIQe92X3lgEAssfyz8B+gIjzxYBzxYTy5/JyIAYAcsFI88WcPoCcQHLaszJgED7AEATyFAE+gJYzxYBzxbMye1UgAIcgCDXIe1E0PoA+kD6QNTRBNMfAYQPIYIQF41FGboCghB73ZfeuhKx8vTTPwEw+gAwE6BQI8hQBPoCWM8WAc8WzMntVIA==").unwrap();
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
