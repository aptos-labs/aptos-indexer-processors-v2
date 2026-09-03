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

impl MemSize for Vec<u8> {
    fn mem_size(&self) -> usize {
        // Heap contents plus the Vec struct itself (ptr/len/cap).
        self.capacity() + size_of_val(self)
    }
}

/// Approximate in-memory size of a Vec of records.
///
/// Each element's `mem_size()` already accounts for both its shallow (inline)
/// size and its heap contents, so we just sum those across the elements. We do
/// NOT add `capacity() * size_of::<T>()` for the Vec's backing allocation: that
/// backing store is exactly the inline shallow size already counted per element,
/// so adding it would double-count the shallow size of every record.
pub fn size_of_records<T: MemSize>(records: &Vec<T>) -> usize {
    let elems: usize = records.iter().map(MemSize::mem_size).sum();
    // Sum of per-record sizes + the Vec struct itself (ptr/len/cap).
    elems + size_of_val(records)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Row {
        id: i64,
        deleted: bool,
        // heap fields
        name: String,
        data: Option<String>,
    }

    impl MemSize for Row {
        fn mem_size(&self) -> usize {
            let shallow = std::mem::size_of_val(self);
            shallow + self.name.mem_size() + self.data.mem_size()
        }
    }

    fn row(name_len: usize, data_len: Option<usize>) -> Row {
        Row {
            id: 1,
            deleted: false,
            name: "a".repeat(name_len),
            data: data_len.map(|n| "b".repeat(n)),
        }
    }

    #[test]
    fn scalar_only_sizes_to_shallow() {
        // A struct with no heap fields sizes to its shallow size.
        assert_eq!(5i64.mem_size(), std::mem::size_of::<i64>());
        assert_eq!(true.mem_size(), std::mem::size_of::<bool>());
        assert_eq!(None::<String>.mem_size(), std::mem::size_of::<Option<String>>());
    }

    #[test]
    fn string_fields_add_heap_content() {
        let small = row(4, None).mem_size();
        let large = row(4000, None).mem_size();
        // The 4000-char string must add ~4000 bytes beyond the shallow size.
        assert!(large > small, "larger string should size larger");
        assert!(
            large >= 4000,
            "string heap contents should be counted, got {large}"
        );
    }

    #[test]
    fn option_some_counts_contents() {
        let none = row(1, None).mem_size();
        let some = row(1, Some(1000)).mem_size();
        assert!(some > none, "Some(large) should exceed None");
        assert!(some - none >= 1000, "should count the Some contents");
    }

    #[test]
    fn size_of_records_is_monotonic_and_scales() {
        let empty: Vec<Row> = Vec::new();
        let one = vec![row(10, Some(10))];
        let ten = vec![row(10, Some(10)); 10];

        let e = size_of_records(&empty);
        let o = size_of_records(&one);
        let t = size_of_records(&ten);

        assert!(o > e, "adding a record should increase size");
        assert!(t > o, "more records should increase size");
        // Ten identical records should size to ~10x one record (plus the shared
        // Vec header). This is the no-double-counting property: each record's
        // shallow size is counted once, inside its own mem_size().
        let per_record = o - e; // one record's mem_size (vec header cancels)
        assert!(
            t >= 10 * per_record,
            "10 records should size ~10x one record: ten={t} one={o} empty={e}"
        );
        // And it should not grossly overshoot (no quadratic/double counting).
        assert!(
            t <= 10 * per_record + e,
            "10 records should not exceed 10 records + one Vec header: ten={t}"
        );
    }

    #[test]
    fn vec_u8_counts_heap_contents() {
        let v: Vec<u8> = vec![0u8; 2048];
        assert!(
            v.mem_size() >= 2048,
            "Vec<u8> should count its backing bytes, got {}",
            v.mem_size()
        );
    }
}
