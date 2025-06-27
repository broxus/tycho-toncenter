use std::cell::RefCell;
use std::fmt::Write;

use everscale_types::cell::HashBytes;
use everscale_types::models::StdAddr;
use num_bigint::BigUint;
use once_cell::race::OnceBox;
use rusqlite::types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef};
use rusqlite::{Row, RowIndex, ToSql};

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

    let mut result = format!("({}),", Tuple(tuple_size)).repeat(tuple_count);
    result.pop();
    result
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

pub const SQLITE_MAX_VARIABLE_NUMBER: usize = 32766 / 4;
