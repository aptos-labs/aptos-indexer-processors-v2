// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! A lightweight replacement for the `allocative` crate.
//!
//! `allocative` is unmaintained and its latest release fails to compile on
//! current Rust nightlies (conflicting `impl Allocative for !` vs
//! `impl Allocative for Infallible`, since `!` is now an alias of
//! `Infallible`). We only ever used it for a soft GCS-flush threshold in
//! `ParquetBufferStep` (`calculate_size`), so we replace it with a small
//! string-aware size estimate instead of forking the crate.
//!
//! The estimate is intentionally approximate: it sums the shallow size of each
//! record plus the heap contents of `String`/`Option<String>` fields. That is
//! close enough for deciding when to flush a buffered Parquet batch to GCS,
//! and it is monotonic and roughly proportional to real heap usage.

use std::mem::size_of_val;

/// Types that can report an approximate in-memory size in bytes.
pub trait MemSize {
    fn mem_size(&self) -> usize;
}

impl MemSize for String {
    fn mem_size(&self) -> usize {
        // Heap contents plus the String struct itself (ptr/len/cap).
        self.capacity() + size_of_val(self)
    }
}

impl MemSize for &str {
    fn mem_size(&self) -> usize {
        self.len()
    }
}

impl<T: MemSize> MemSize for Option<T> {
    fn mem_size(&self) -> usize {
        match self {
            Some(v) => size_of_val(self) + v.mem_size(),
            None => size_of_val(self),
        }
    }
}

macro_rules! impl_mem_size_for_scalar {
    ($($t:ty),* $(,)?) => {
        $(
            impl MemSize for $t {
                fn mem_size(&self) -> usize {
                    size_of_val(self)
                }
            }
        )*
    };
}

impl_mem_size_for_scalar!(bool, i32, i64, u64, chrono::NaiveDateTime);

/// Approximate in-memory size of a slice of records.
///
/// Includes the shallow size of each element plus each element's heap usage,
/// plus the slice's own allocation overhead.
pub fn size_of_records<T: MemSize>(records: &Vec<T>) -> usize {
    let elems: usize = records.iter().map(MemSize::mem_size).sum();
    // Element heap sizes + the Vec's own backing allocation + Vec struct.
    elems + records.capacity() * std::mem::size_of::<T>() + size_of_val(records)
}

/// Derives a `MemSize` impl for a flat struct by summing the sizes of the
/// listed fields. Only field types that implement `MemSize` are allowed.
///
/// Usage:
///   impl_mem_size!(ParquetMoveResource, resource_address, resource_type, ...);
#[macro_export]
macro_rules! impl_mem_size {
    // No heap fields: shallow size only.
    ($type:ty) => {
        impl $crate::parquet_processors::parquet_utils::mem_size::MemSize for $type {
            fn mem_size(&self) -> usize {
                std::mem::size_of_val(self)
            }
        }
    };
    // One or more heap (String / Option<String>) fields.
    ($type:ty, $($field:ident),+ $(,)?) => {
        impl $crate::parquet_processors::parquet_utils::mem_size::MemSize for $type {
            fn mem_size(&self) -> usize {
                let shallow = std::mem::size_of_val(self);
                shallow $(+ $crate::parquet_processors::parquet_utils::mem_size::MemSize::mem_size(&self.$field))+
            }
        }
    };
}
