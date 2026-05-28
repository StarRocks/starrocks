// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for FFI result types — verify the alloc/free shape contract for
//! `RustStringArray`, which previously round-tripped through
//! `Vec::from_raw_parts(ptr, len, len)` and could trigger UB on jemalloc-style
//! allocators that over-allocate small `with_capacity(n)` requests.

use crate::ffi::lifecycle_c::tantivy_free_string_array;
use crate::ffi::result::RustStringArray;

#[test]
fn empty_array_roundtrip() {
    let array = RustStringArray::from_strings(Vec::new());
    assert!(array.ptr.is_null());
    assert_eq!(array.len, 0);
    unsafe { tantivy_free_string_array(array) };
}

#[test]
fn single_string_roundtrip() {
    let array = RustStringArray::from_strings(vec!["hello".to_string()]);
    assert!(!array.ptr.is_null());
    assert_eq!(array.len, 1);
    unsafe { tantivy_free_string_array(array) };
}

#[test]
fn many_small_strings_roundtrip() {
    // Exercise a size that an allocator like jemalloc is likely to round up:
    // 9 * sizeof(*mut c_char) = 72 bytes on 64-bit. jemalloc's small-class
    // bucket is typically 80 or 96 bytes, so `Vec::with_capacity(9)` would
    // over-allocate, which is exactly the failure mode the boxed-slice path
    // must defend against.
    let strings: Vec<String> = (0..9).map(|i| format!("token-{}", i)).collect();
    let array = RustStringArray::from_strings(strings);
    assert_eq!(array.len, 9);
    unsafe { tantivy_free_string_array(array) };
}

#[test]
fn many_strings_roundtrip_stress() {
    for batch in 0..32 {
        let strings: Vec<String> = (0..batch).map(|i| format!("s{}", i)).collect();
        let array = RustStringArray::from_strings(strings);
        assert_eq!(array.len, batch);
        unsafe { tantivy_free_string_array(array) };
    }
}

#[test]
fn unicode_strings_roundtrip() {
    let strings = vec![
        "你好".to_string(),
        "世界".to_string(),
        "tantivy".to_string(),
        "🦀".to_string(),
    ];
    let array = RustStringArray::from_strings(strings);
    assert_eq!(array.len, 4);
    unsafe { tantivy_free_string_array(array) };
}

#[test]
fn string_with_interior_nul_sanitized() {
    // CString::new fails on interior NUL; from_strings replaces with empty.
    let array = RustStringArray::from_strings(vec!["a\0b".to_string()]);
    assert_eq!(array.len, 1);
    unsafe { tantivy_free_string_array(array) };
}
