use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};
use num_bigint::{BigInt, BigUint};
use sha2::Digest;
use tycho_rpc::{GenTimings, RpcState};
use tycho_types::models::{
    Account, AccountState, BlockchainConfigParams, CurrencyCollection, IntAddr, LibDescr,
    ShardAccount, StdAddr,
};
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_util::FastHashMap;
use tycho_vm::{
    BehaviourModifiers, GasParams, OwnedCellSlice, RcStackValue, SafeRc, SmcInfoTonV6, Stack,
    StackValueType, UnpackedConfig,
};

// === Method ID stuff ===

pub fn compute_method_id(bytes: impl AsRef<[u8]>) -> i64 {
    tycho_types::crc::crc_16(bytes.as_ref()) as i64 | 0x10000
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
        compute_method_id(self)
    }
}

impl MethodId for String {
    #[inline]
    fn compute_id(&self) -> i64 {
        self.as_str().compute_id()
    }
}

// === Stack parser ==

pub trait FromStack: Sized {
    fn from_stack(stack: Stack) -> Result<Self>;

    fn field_count_hint() -> Option<usize>;
}

pub struct StackParser {
    items: Vec<RcStackValue>,
    origin: StackParseOrigin,
}

impl StackParser {
    pub fn begin(items: impl IntoStackItems, origin: StackParseOrigin) -> Self {
        let mut items = items.into_stack_items();
        if origin == StackParseOrigin::Bottom {
            items.reverse();
        }
        Self { items, origin }
    }

    pub fn begin_from_bottom(items: impl IntoStackItems) -> Self {
        Self::begin(items, StackParseOrigin::Bottom)
    }

    #[allow(unused)]
    pub fn begin_from_top(items: impl IntoStackItems) -> Self {
        Self::begin(items, StackParseOrigin::Top)
    }

    #[allow(unused)]
    pub fn set_origin(&mut self, origin: StackParseOrigin) {
        if self.origin != origin {
            self.items.reverse();
            self.origin = origin;
        }
    }

    pub fn pop_int(&mut self) -> Result<BigInt> {
        self.pop_item()?
            .into_int()
            .map(SafeRc::unwrap_or_clone)
            .map_err(Into::into)
    }

    pub fn pop_uint(&mut self) -> Result<BigUint> {
        let (sign, int) = self.pop_int()?.into_parts();
        anyhow::ensure!(sign != num_bigint::Sign::Minus, "expected non-negative int");
        Ok(int)
    }

    pub fn pop_bool(&mut self) -> Result<bool> {
        let int = self.pop_int()?;
        Ok(int.sign() != num_bigint::Sign::NoSign)
    }

    pub fn pop_cell(&mut self) -> Result<Cell> {
        self.pop_item()?
            .into_cell()
            .map(SafeRc::unwrap_or_clone)
            .map_err(Into::into)
    }

    pub fn pop_cell_or_slice(&mut self) -> Result<OwnedCellSlice> {
        // TODO: Move aliases into VM.
        const CELL_TY: u8 = StackValueType::Cell as u8;
        const SLICE_TY: u8 = StackValueType::Slice as u8;

        let item = self.pop_item()?;
        Ok(match item.raw_ty() {
            CELL_TY => {
                let cell = item.into_cell()?;
                OwnedCellSlice::new_allow_exotic(SafeRc::unwrap_or_clone(cell))
            }
            SLICE_TY => SafeRc::unwrap_or_clone(item.into_cell_slice()?),
            ty => anyhow::bail!("expected cell or slice, got {ty:?}"),
        })
    }

    pub fn pop_address(&mut self) -> Result<StdAddr> {
        StdAddr::load_from(&mut self.pop_cell_or_slice()?.apply()).map_err(Into::into)
    }

    pub fn pop_address_or_none(&mut self) -> Result<Option<StdAddr>> {
        let slice = self.pop_cell_or_slice()?;
        let mut cs = slice.apply();
        if cs.get_small_uint(0, 2)? == 0b00 {
            Ok(None)
        } else {
            StdAddr::load_from(&mut cs).map(Some).map_err(Into::into)
        }
    }

    pub fn pop_item(&mut self) -> Result<RcStackValue> {
        match self.items.pop() {
            Some(item) => Ok(item),
            None => anyhow::bail!("not enough items on stack"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackParseOrigin {
    Bottom,
    Top,
}

// === Into stack items ===

pub trait IntoStackItems {
    fn into_stack_items(self) -> Vec<RcStackValue>;
}

impl IntoStackItems for SafeRc<Stack> {
    fn into_stack_items(self) -> Vec<RcStackValue> {
        SafeRc::unwrap_or_clone(self).items
    }
}

impl IntoStackItems for Stack {
    #[inline]
    fn into_stack_items(self) -> Vec<RcStackValue> {
        self.items
    }
}

impl IntoStackItems for Vec<RcStackValue> {
    #[inline]
    fn into_stack_items(self) -> Vec<RcStackValue> {
        self
    }
}

// === Jetton Attribute ===

pub enum TokenDataAttributes {
    Onchain {
        data: FastHashMap<TokenDataAttribute, String>,
    },
    Offchain {
        data: String,
    },
}

impl TokenDataAttributes {
    pub fn parse_value(mut value: CellSlice<'_>) -> Result<Vec<u8>, tycho_types::error::Error> {
        if value.is_data_empty() {
            value = value.load_reference_as_slice()?;
        }

        match value.load_u8()? {
            0x00 => load_bytes_rope(value, false),
            0x01 => {
                let dict = Dict::<u32, Cell>::load_from(&mut value)?;
                let mut data = Vec::new();
                for item in dict.values() {
                    let item = item?;
                    data.extend_from_slice(&load_bytes_rope(item.as_slice()?, false)?);
                }
                Ok(data)
            }
            _ => Err(tycho_types::error::Error::InvalidTag),
        }
    }
}

impl<'a> Load<'a> for TokenDataAttributes {
    fn load_from(cs: &mut CellSlice<'a>) -> Result<Self, tycho_types::error::Error> {
        match cs.load_u8()? {
            0x00 => {
                let mut data = FastHashMap::default();
                let dict = Dict::<HashBytes, CellSlice<'_>>::load_from(cs)?;
                for item in dict.iter() {
                    let (name, value) = item?;
                    let value = Self::parse_value(value)?;
                    data.insert(
                        TokenDataAttribute::resolve(&name),
                        String::from_utf8_lossy(&value).into_owned(),
                    );
                }
                Ok(Self::Onchain { data })
            }
            0x01 => {
                let data = load_bytes_rope(*cs, false)?;
                Ok(Self::Offchain {
                    data: String::from_utf8_lossy(&data).into_owned(),
                })
            }
            _ => Err(tycho_types::error::Error::InvalidTag),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TokenDataAttribute {
    Known(&'static str),
    Unknown(HashBytes),
}

impl TokenDataAttribute {
    pub const KNOWN_ATTRIBUTES: [&str; 9] = [
        "uri",
        "name",
        "description",
        "image",
        "image_data",
        "symbol",
        "decimals",
        "amount_style",
        "render_type",
    ];

    pub fn resolve(hash: &HashBytes) -> Self {
        static KNOWN: OnceLock<FastHashMap<HashBytes, &'static str>> = OnceLock::new();
        let known = KNOWN.get_or_init(|| {
            let mut result = FastHashMap::with_capacity_and_hasher(
                Self::KNOWN_ATTRIBUTES.len(),
                Default::default(),
            );
            for name in Self::KNOWN_ATTRIBUTES {
                let hash = sha2::Sha256::digest(name);
                result.insert(HashBytes(hash.into()), name);
            }
            result
        });
        match known.get(hash).copied() {
            Some(name) => Self::Known(name),
            None => Self::Unknown(*hash),
        }
    }
}

impl std::fmt::Display for TokenDataAttribute {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Known(s) => f.write_str(s),
            Self::Unknown(s) => std::fmt::Display::fmt(s, f),
        }
    }
}

impl serde::Serialize for TokenDataAttribute {
    #[inline]
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

pub fn load_bytes_rope(
    mut cs: CellSlice<'_>,
    strict: bool,
) -> Result<Vec<u8>, tycho_types::error::Error> {
    let mut result = Vec::new();
    let mut buffer = [0u8; 128];
    loop {
        // TODO: Should we trim unaligned bytes?
        let bytes = cs.load_raw(&mut buffer, cs.size_bits())?;
        result.extend_from_slice(bytes);

        match cs.size_refs() {
            0 => break,
            2.. if strict => return Err(tycho_types::error::Error::InvalidData),
            _ => cs = cs.load_reference_as_slice()?,
        }
    }
    Ok(result)
}

// === Simple Executor ===

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
    ) -> Result<Self, tycho_types::error::Error> {
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
        let Some(descr) = self.libraries.get(lib_hash)? else {
            return Err(tycho_types::error::Error::CellUnderflow);
        };

        Ok(descr.lib)
    }

    pub fn run_getter<R: FromStack>(
        &self,
        account: &Account,
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

        if let Some(require_fields) = R::field_count_hint()
            && stack.items.len() < require_fields
        {
            return Err(ExecutorError::FailedToParse(anyhow!(
                "too few stack arguments"
            )));
        }

        R::from_stack(SafeRc::unwrap_or_clone(stack)).map_err(ExecutorError::FailedToParse)
    }

    // TODO: Use in toncenter V2 API.
    pub fn run_getter_raw(
        &self,
        account: &Account,
        params: RunGetterParams,
    ) -> Result<VmOutput, ExecutorError> {
        let IntAddr::Std(address) = &account.address else {
            return Err(ExecutorError::StateAccess(
                tycho_types::error::Error::InvalidTag,
            ));
        };
        let AccountState::Active(state_init) = &account.state else {
            return Err(ExecutorError::AccountNotActive);
        };

        let Some(code) = &state_init.code else {
            return Ok(VmOutput::no_code());
        };

        // Prepare VM state.
        let (gas, stack) = params.build();

        let smc_info = tycho_vm::SmcInfoBase::new()
            .with_now(self.timings.gen_utime)
            .with_block_lt(self.timings.gen_lt)
            .with_tx_lt(self.timings.gen_lt)
            .with_account_balance(account.balance.clone())
            .with_account_addr(address.clone().into())
            .with_config(self.raw_config.clone())
            .require_ton_v4()
            .with_code(code.clone())
            .with_message_balance(CurrencyCollection::ZERO)
            .with_storage_fees(Tokens::ZERO)
            .require_ton_v6()
            .with_unpacked_config(self.unpacked_config.as_tuple())
            .require_ton_v11();

        let libraries = (&state_init.libraries, &self.libraries);
        let mut vm = tycho_vm::VmState::builder()
            .with_smc_info(smc_info)
            .with_code(code.clone())
            .with_data(state_init.data.clone().unwrap_or_default())
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
            stack,
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

    pub fn with_args<I: IntoIterator<Item = RcStackValue> + 'static>(mut self, args: I) -> Self {
        self.args = match castaway::cast!(args, Vec<RcStackValue>) {
            Ok(args) => args,
            Err(args) => args.into_iter().collect(),
        };
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

#[derive(Clone)]
pub struct VmOutput {
    pub exit_code: i32,
    pub stack: SafeRc<Stack>,
    pub gas_used: u64,
}

impl VmOutput {
    pub fn no_code() -> Self {
        thread_local! {
            static NO_CODE: VmOutput = VmOutput {
                exit_code: -14,
                stack: Default::default(),
                gas_used: 0,
            };
        }

        NO_CODE.with(Clone::clone)
    }
}

pub fn run_getter(
    state: &RpcState,
    account_state: &ShardAccount,
    timings: GenTimings,
    method_id: i64,
    stack: Vec<RcStackValue>,
    gas_limit: u64,
) -> Result<VmOutput, tycho_types::error::Error> {
    let Some(account) = account_state.load_account()? else {
        return Ok(VmOutput::no_code());
    };

    let config = state.get_unpacked_blockchain_config();

    SimpleExecutor {
        libraries: state.get_libraries(),
        raw_config: config.raw.clone(),
        unpacked_config: config.unpacked.clone(),
        timings,
        modifiers: config.modifiers,
    }
    .run_getter_raw(
        &account,
        RunGetterParams::new(method_id)
            .with_args(stack)
            .with_gas_limit(gas_limit),
    )
    .or_else(|e| match e {
        ExecutorError::AccountNotActive => Ok(VmOutput::no_code()),
        ExecutorError::StateAccess(e) => Err(e),
        ExecutorError::FailedToParse(_) => Err(tycho_types::error::Error::InvalidData),
    })
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

pub fn parse_vm_bigint<E: serde::de::Error>(value: impl AsRef<str>) -> Result<BigInt, E> {
    use serde::de::Error;

    struct IntBounds {
        min: BigInt,
        max: BigInt,
    }

    impl IntBounds {
        fn get() -> &'static Self {
            static BOUNDS: OnceLock<IntBounds> = OnceLock::new();
            BOUNDS.get_or_init(|| Self {
                min: BigInt::from(-1) << 256,
                max: (BigInt::from(1) << 256) - 1,
            })
        }

        fn contains(&self, int: &BigInt) -> bool {
            *int >= self.min && *int <= self.max
        }
    }

    const MAX_INT_LEN: usize = 81;

    let value = value.as_ref();
    if value.len() > MAX_INT_LEN {
        return Err(Error::invalid_length(
            value.len(),
            &"a decimal integer in range [-2^256, 2^256)",
        ));
    }

    let int = if let Some((sign, value)) = value
        .strip_prefix("-0x")
        .map(|hex| (num_bigint::Sign::Minus, hex))
        .or_else(|| {
            value
                .strip_prefix("0x")
                .map(|hex| (num_bigint::Sign::Plus, hex))
        }) {
        if value.is_empty() {
            return Err(Error::custom("empty hex integer"));
        }

        let Some(magnitude) = BigUint::parse_bytes(value.as_bytes(), 16) else {
            return Err(Error::custom("invalid hex integer"));
        };
        BigInt::from_biguint(sign, magnitude)
    } else {
        BigInt::from_str(value).map_err(Error::custom)?
    };

    if !IntBounds::get().contains(&int) {
        return Err(Error::custom("integer out of bounds"));
    }
    Ok(int)
}

pub fn parse_cell_mapped<T, F: FnOnce(Cell) -> T, E: serde::de::Error>(
    value: impl AsRef<str>,
    f: F,
) -> Result<T, E> {
    Boc::decode_base64(value.as_ref())
        .map(f)
        .map_err(serde::de::Error::custom)
}

#[repr(transparent)]
pub struct LimitStackItems<T>(T);

impl<T> LimitStackItems<T> {
    pub fn new(data: T, limit: usize) -> Self {
        STACK_ITEMS_LIMIT.set(limit);
        Self(data)
    }
}

impl LimitStackItems<()> {
    pub fn use_limit<E: serde::ser::Error>() -> Result<(), E> {
        let is_limit_ok = STACK_ITEMS_LIMIT.with(|limit| {
            let current_limit = limit.get();
            if current_limit > 0 {
                limit.set(current_limit - 1);
                true
            } else {
                false
            }
        });

        if is_limit_ok {
            Ok(())
        } else {
            Err(E::custom("too many stack items in response"))
        }
    }
}

impl<T: serde::ser::Serialize> serde::ser::Serialize for LimitStackItems<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

thread_local! {
    static STACK_ITEMS_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub fn limit_tuple_depth<E: serde::ser::Error>()
-> Result<scopeguard::ScopeGuard<(), impl FnOnce(())>, E> {
    const MAX_DEPTH: usize = 16;

    thread_local! {
        static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    let is_depth_ok = DEPTH.with(|depth| {
        let current_depth = depth.get();
        if current_depth < MAX_DEPTH {
            depth.set(current_depth + 1);
            true
        } else {
            false
        }
    });

    if !is_depth_ok {
        return Err(E::custom("too deep stack item"));
    }

    let guard = scopeguard::guard((), |()| {
        DEPTH.with(|depth| {
            depth.set(depth.get() - 1);
        })
    });

    Ok(guard)
}
