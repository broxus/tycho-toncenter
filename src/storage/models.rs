use std::num::NonZeroUsize;

use everscale_types::cell::HashBytes;
use everscale_types::models::StdAddr;
use num_bigint::BigUint;

use super::util::*;

// === Rows ===

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

// === Params ===

pub struct GetJettonMastersParams {
    pub master_addresses: Option<Vec<StdAddr>>,
    pub admin_addresses: Option<Vec<StdAddr>>,
    pub limit: NonZeroUsize,
    pub offset: usize,
}
