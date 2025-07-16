use std::cell::RefCell;
use std::fmt::Write;

use num_bigint::BigUint;
use once_cell::race::OnceBox;
use rusqlite::types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef};
use rusqlite::{Row, RowIndex, ToSql};
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, ShardIdent, StdAddr};

macro_rules! row {
    (
        $(#[$($meta:tt)*])*
        $pub:vis struct $ident:ident {
            $(
                $(#[$($field_meta:tt)*])*
                $field_pub:vis $field:ident: $field_ty:ty
            ),*$(,)?
        }
    ) => {
        $(#[$($meta)*])*
        $pub struct $ident {
            $(
            $(#[$($field_meta)*])*
            $field_pub $field: $field_ty,
            )*
        }

        impl $crate::state::util::KnownColumnCount for $ident {
            const COLUMN_COUNT: usize = const {
                $crate::state::util::row!(@field_count { 0 } $($field)*)
            };

            fn batch_params_string() -> &'static str {
                static STR: ::once_cell::race::OnceBox<String> = ::once_cell::race::OnceBox::new();
                STR.get_or_init(|| {
                    Box::new($crate::state::util::tuple_list(Self::max_rows_per_batch(), Self::COLUMN_COUNT))
                })
            }
        }

        impl<'stmt> TryFrom<&::rusqlite::Row<'stmt>> for $ident {
            type Error = ::rusqlite::Error;

            fn try_from(row: &::rusqlite::Row<'stmt>) -> Result<Self, Self::Error> {
                Ok(row!(@fields row {} { 0 } $($field)*))
            }
        }

        impl $crate::state::util::SqlColumnsRepr for $ident {
            type Iter<'a> = [&'a dyn ::rusqlite::ToSql; Self::COLUMN_COUNT]
            where
                Self: 'a;

            fn as_columns_iter(&self) -> Self::Iter<'_> {
                [$($crate::state::util::SqlType::wrap(&self.$field) as &dyn ::rusqlite::ToSql),*]
            }
        }
    };

    (@fields $row:ident { $($fields:tt)* } { $idx:expr }) => {
        Self { $($fields)* }
    };
    (@fields $row:ident { $($fields:tt)* } { $idx:expr } $field:ident $($rest:ident)*) => {
        $crate::state::util::row!(@fields $row {
            $($fields)*
            $field: $crate::state::util::SqlType::get($row, const { $idx })?,
        } {
            $idx + 1
        } $($rest)*)
    };

    (@field_count { $idx:expr }) => { $idx };
    (@field_count { $idx:expr } $field:ident $($rest:ident)*) => {
        $crate::state::util::row!(@field_count { $idx + 1 } $($rest)*)
    };
}

pub(crate) use row;

// === Buffer ===

pub struct QueryBuffer;

impl QueryBuffer {
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&mut String) -> R,
    {
        thread_local! {
            static QUERY_BUFFER: RefCell<String> = const { RefCell::new(String::new()) };
        }

        QUERY_BUFFER.with_borrow_mut(|buffer| {
            buffer.clear();
            f(buffer)
        })
    }
}

// === Params ===

pub fn skip_or_where<T>(filter: &Option<T>) -> impl std::fmt::Display + '_
where
    T: std::ops::Deref<Target = str>,
{
    struct OptWhere<'a>(Option<&'a str>);

    impl std::fmt::Display for OptWhere<'_> {
        #[inline]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                None => Ok(()),
                Some(filter) => write!(f, "WHERE {filter}"),
            }
        }
    }

    OptWhere(filter.as_deref())
}

pub fn add_opt_array_filter<'a, T>(
    filter: &mut Option<String>,
    field: &str,
    items: Option<&'a [T]>,
) -> &'a [T] {
    if let Some(items) = items {
        if let Some(new_filter) = array_filter_params(field, items.len()) {
            match filter {
                None => *filter = Some(new_filter),
                Some(filter) => write!(filter, " AND {new_filter}").unwrap(),
            }
            return items;
        }
    }
    &[]
}

pub fn add_array_filter<'a, T>(
    filter: &mut Option<String>,
    field: &str,
    items: &'a [T],
) -> &'a [T] {
    if let Some(new_filter) = array_filter_params(field, items.len()) {
        match filter {
            None => *filter = Some(new_filter),
            Some(filter) => write!(filter, " AND {new_filter}").unwrap(),
        }
        return items;
    }
    &[]
}

pub fn add_raw_filter(filter: &mut Option<String>, raw: &str) {
    match filter {
        None => *filter = Some(raw.to_owned()),
        Some(filter) => write!(filter, " AND {raw}").unwrap(),
    }
}

pub fn array_filter_params(field: &str, item_count: usize) -> Option<String> {
    match item_count {
        0 => None,
        1 => Some(format!("{field} = ?")),
        _ => Some(format!("{field} IN ({})", param_list(item_count))),
    }
}

pub fn tuple_list(tuple_count: usize, tuple_size: usize) -> String {
    struct Tuple(usize);

    impl std::fmt::Display for Tuple {
        #[inline]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if self.0 > 0 {
                f.write_str("?")?;
            }
            for _ in 1..self.0 {
                f.write_str(",?")?;
            }
            Ok(())
        }
    }

    if tuple_size == 1 {
        param_list(tuple_count).to_owned()
    } else {
        let mut result = format!("({}),", Tuple(tuple_size)).repeat(tuple_count);
        result.pop();
        result
    }
}

pub fn param_list(param_count: usize) -> &'static str {
    static LIST: OnceBox<String> = OnceBox::new();

    assert!(param_count <= SQLITE_MAX_VARIABLE_NUMBER);

    match param_count {
        0 => "",
        1 => "?",
        _ => {
            let list = LIST
                .get_or_init(|| Box::new("?,".repeat(SQLITE_MAX_VARIABLE_NUMBER)))
                .as_str();
            &list[0..param_count * 2 - 1]
        }
    }
}

// === SQL Columns wrapper ===

pub trait SqlColumnsRepr {
    type Iter<'a>: IntoIterator<Item = &'a dyn ToSql>
    where
        Self: 'a;

    fn as_columns_iter(&self) -> Self::Iter<'_>;
}

pub trait KnownColumnCount {
    const COLUMN_COUNT: usize;

    fn max_rows_per_batch() -> usize {
        SQLITE_MAX_VARIABLE_NUMBER / Self::COLUMN_COUNT
    }

    fn batch_params_string() -> &'static str;
}

macro_rules! tuple_row {
    ($field_count:literal, $params:literal, $($t:ident: $n:tt),+$(,)?) => {
        impl<$($t),+> $crate::state::util::KnownColumnCount for ($($t,)+) {
            const COLUMN_COUNT: usize = $field_count;

            fn batch_params_string() -> &'static str {
                static STR: ::once_cell::race::OnceBox<String> = ::once_cell::race::OnceBox::new();
                STR.get_or_init(|| {
                    Box::new($crate::state::util::tuple_list(Self::max_rows_per_batch(), Self::COLUMN_COUNT))
                })
            }
        }

        impl<$($t),+> $crate::state::util::SqlColumnsRepr for ($($t,)+)
        where
            $($t: $crate::state::util::SqlTypeRepr,)+
        {
            type Iter<'a> = [&'a dyn ::rusqlite::ToSql; $field_count]
            where
                Self: 'a;

            fn as_columns_iter(&self) -> Self::Iter<'_> {
                [$($crate::state::util::SqlType::wrap(&self.$n) as &dyn ::rusqlite::ToSql),*]
            }
        }
    };
}

tuple_row!(1, "?", T0: 0);
tuple_row!(2, "?,?", T0: 0, T1: 1);
tuple_row!(3, "?,?,?", T0: 0, T1: 1, T2: 2);
tuple_row!(4, "?,?,?,?", T0: 0, T1: 1, T2: 2, T3: 3);
tuple_row!(5, "?,?,?,?,?", T0: 0, T1: 1, T2: 2, T3: 3, T4: 4);
tuple_row!(6, "?,?,?,?,?,?", T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5);
tuple_row!(7, "?,?,?,?,?,?,?", T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6);

impl SqlColumnsRepr for StdAddr {
    type Iter<'a>
        = [&'a dyn ::rusqlite::ToSql; 1]
    where
        Self: 'a;

    #[inline]
    fn as_columns_iter(&self) -> Self::Iter<'_> {
        [SqlType::wrap(self) as &dyn ::rusqlite::ToSql]
    }
}

impl KnownColumnCount for StdAddr {
    const COLUMN_COUNT: usize = 1;

    fn batch_params_string() -> &'static str {
        param_list(Self::max_rows_per_batch())
    }
}

// === SQL Type wrapper ===

#[repr(transparent)]
pub struct SqlType<T>(pub T);

impl<T> SqlType<T> {
    #[inline]
    pub const fn wrap(value: &T) -> &Self {
        // SAFETY: `SqlType` has the same layout as `T`.
        unsafe { &*(value as *const T).cast::<Self>() }
    }

    pub fn get<I>(row: &Row<'_>, idx: I) -> rusqlite::Result<T>
    where
        I: RowIndex,
        T: SqlTypeRepr,
    {
        let value = row.get_ref(idx)?;
        T::from_sql_impl(value).map_err(Into::into)
    }
}

impl<T: SqlTypeRepr> ToSql for SqlType<T> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql_impl()
    }
}

impl<T: SqlTypeRepr> FromSql for SqlType<T> {
    fn column_result(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        T::from_sql_impl(value).map(Self)
    }
}

pub trait SqlTypeRepr: Sized {
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>>;
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError>;
}

impl<T: SqlTypeRepr> SqlTypeRepr for Option<T> {
    #[inline]
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Self::None => Ok(ToSqlOutput::Borrowed(ValueRef::Null)),
            Self::Some(value) => value.to_sql_impl(),
        }
    }

    #[inline]
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        if matches!(&value, ValueRef::Null) {
            Ok(None)
        } else {
            T::from_sql_impl(value).map(Some)
        }
    }
}

impl SqlTypeRepr for BigUint {
    #[inline]
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Blob(self.to_bytes_be())))
    }

    #[inline]
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        let blob = value.as_blob()?;
        Ok(BigUint::from_bytes_be(blob))
    }
}

impl SqlTypeRepr for StdAddr {
    #[inline]
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let mut bytes = [0u8; ADDR_BYTES];
        bytes[0] = self.workchain as u8;
        bytes[1..33].copy_from_slice(self.address.as_array());
        Ok(ToSqlOutput::Owned(Value::Blob(bytes.to_vec())))
    }

    #[inline]
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        let blob = value.as_blob()?;
        let [workchain, address @ ..]: [u8; ADDR_BYTES] =
            blob.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
                expected_size: ADDR_BYTES,
                blob_size: blob.len(),
            })?;
        Ok(StdAddr::new(workchain as i8, HashBytes(address)))
    }
}

impl SqlTypeRepr for BlockId {
    #[inline]
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let mut bytes = [0u8; BLOCK_ID_BYTES];
        bytes[0..4].copy_from_slice(&self.shard.workchain().to_be_bytes());
        bytes[4..12].copy_from_slice(&self.shard.prefix().to_be_bytes());
        bytes[12..16].copy_from_slice(&self.seqno.to_be_bytes());
        bytes[16..48].copy_from_slice(self.root_hash.as_array());
        bytes[48..80].copy_from_slice(self.file_hash.as_array());
        Ok(ToSqlOutput::Owned(Value::Blob(bytes.to_vec())))
    }

    #[inline]
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        let blob = value.as_blob()?;
        let bytes: [u8; BLOCK_ID_BYTES] =
            blob.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
                expected_size: BLOCK_ID_BYTES,
                blob_size: blob.len(),
            })?;

        let workchain = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let prefix = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        let Some(shard) = ShardIdent::new(workchain, prefix) else {
            return Err(FromSqlError::OutOfRange(prefix as i64));
        };

        Ok(BlockId {
            shard,
            seqno: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            root_hash: HashBytes::from_slice(&bytes[16..48]),
            file_hash: HashBytes::from_slice(&bytes[48..80]),
        })
    }
}

impl SqlTypeRepr for HashBytes {
    #[inline]
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.as_slice())))
    }

    #[inline]
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        let blob = value.as_blob()?;
        blob.try_into()
            .map(HashBytes)
            .map_err(|_| FromSqlError::InvalidBlobSize {
                expected_size: 32,
                blob_size: blob.len(),
            })
    }
}

impl SqlTypeRepr for crate::state::interface::InterfaceType {
    #[inline]
    fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        let id = u8::from_sql_impl(value)?;
        Self::from_id(id).ok_or_else(|| FromSqlError::OutOfRange(id as i64))
    }

    #[inline]
    fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(*self as u8 as i64)))
    }
}

macro_rules! impl_existing {
    ($($ty:ty),*$(,)?) => {
        $(impl SqlTypeRepr for $ty {
            #[inline]
            fn to_sql_impl(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                ToSql::to_sql(self)
            }

            #[inline]
            fn from_sql_impl(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
                FromSql::column_result(value)
            }
        })*
    };
}

impl_existing! {
    bool,
    i8, u8,
    i16, u16,
    i32, u32,
    i64, u64,
    isize, usize,
    String,
    Vec<u8>,
}

pub const ADDR_BYTES: usize = 33;
pub const BLOCK_ID_BYTES: usize = 4 + 8 + 4 + 32 + 32;

pub const SQLITE_MAX_VARIABLE_NUMBER: usize = 32766 / 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_sql() {
        let addr = "0:caab0a342f46d0f32d478a0e90c4ffd61e727ad2b838ea4c2a5825a484960b54"
            .parse::<StdAddr>()
            .unwrap();

        let ToSqlOutput::Owned(blob) = addr.to_sql_impl().unwrap() else {
            unreachable!();
        };

        let from_sql = StdAddr::from_sql_impl(ValueRef::from(&blob)).unwrap();
        assert_eq!(addr, from_sql);
    }

    #[test]
    fn block_id_sql() {
        let block_id = "-1:8000000000000000:4148544:\
            57142cda6e2aed8df9f11f9312e82da7c8c650f5cba0eba9b7f49a350ba9e417:\
            4ae138982d446d74052a55a07a0511b2f68d628510cb53195243561c23053f1a"
            .parse::<BlockId>()
            .unwrap();

        let ToSqlOutput::Owned(blob) = block_id.to_sql_impl().unwrap() else {
            unreachable!();
        };

        let from_sql = BlockId::from_sql_impl(ValueRef::from(&blob)).unwrap();
        assert_eq!(block_id, from_sql);
    }
}
