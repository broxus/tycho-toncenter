use std::num::NonZeroUsize;

use num_bigint::BigUint;
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;

use super::parser::InterfaceType;
use super::util::*;

// === Rows ===

row! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct KnownInterface {
        pub code_hash: HashBytes,
        pub interface: u8,
        pub is_broken: bool,
    }
}

// TODO: Split into "new" and "updated" models?
row! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct JettonMaster {
        pub address: StdAddr,
        pub total_supply: BigUint,
        pub mintable: bool,
        pub admin_address: Option<StdAddr>,
        pub jetton_content: Option<String>,
        pub wallet_code_hash: HashBytes,
        pub last_transaction_lt: u64,
        pub code_hash: HashBytes,
        pub data_hash: HashBytes,
    }
}

impl JettonMaster {
    pub fn as_known_interface(&self) -> KnownInterface {
        KnownInterface {
            code_hash: self.code_hash,
            interface: InterfaceType::JettonMaster as _,
            is_broken: false,
        }
    }
}

// TODO: Split into "new" and "updated" models?
row! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct JettonWallet {
        pub address: StdAddr,
        pub balance: BigUint,
        pub owner: StdAddr,
        pub jetton: StdAddr,
        pub last_transaction_lt: u64,
        pub code_hash: HashBytes,
        pub data_hash: HashBytes,
    }
}

impl JettonWallet {
    pub fn as_known_interface(&self) -> KnownInterface {
        KnownInterface {
            code_hash: self.code_hash,
            interface: InterfaceType::JettonWallet as _,
            is_broken: false,
        }
    }
}

row! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct BriefJettonWalletInfo {
        pub address: StdAddr,
        pub owner: StdAddr,
        pub jetton: StdAddr,
    }
}

// === Params ===

pub struct GetJettonMastersParams {
    pub master_addresses: Option<Vec<StdAddr>>,
    pub admin_addresses: Option<Vec<StdAddr>>,
    pub limit: NonZeroUsize,
    pub offset: usize,
}

pub struct GetJettonWalletsParams {
    pub wallet_addresses: Option<Vec<StdAddr>>,
    pub owner_addresses: Option<Vec<StdAddr>>,
    pub jetton_addresses: Option<Vec<StdAddr>>,
    pub exclude_zero_balance: bool,
    pub limit: NonZeroUsize,
    pub offset: usize,
    pub order_by: Option<OrderJettonWalletsBy>,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderJettonWalletsBy {
    Balance { reverse: bool },
}
