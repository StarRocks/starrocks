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

//! C ABI result types.
//!
//! Every fallible FFI function returns a `RustResult`. On the C++ side this is
//! a tagged union: inspect `success`, then read `value` according to `tag`, and
//! always free the result with `free_rust_result` to release the embedded error
//! string and any owned array.
//!
//! We deliberately use a tag + plain struct (not a Rust `union`) because
//! cbindgen renders Rust enums-with-data as C++ tagged structs cleanly, and
//! this layout is easier to debug from C++.

use std::ffi::{c_char, c_void, CString};

use crate::error::TantivyBindingError;

/// A `{ptr, len}` slice passed from C++ to Rust. Does not own the memory.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFISlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl FFISlice {
    /// Interpret as a `&str`, validating UTF-8.
    ///
    /// SAFETY: caller must guarantee `ptr` points to `len` readable bytes.
    pub unsafe fn as_str(&self) -> std::result::Result<&str, TantivyBindingError> {
        if self.ptr.is_null() {
            return Ok("");
        }
        let bytes = std::slice::from_raw_parts(self.ptr, self.len);
        std::str::from_utf8(bytes)
            .map_err(|e| TantivyBindingError::InvalidArgument(format!("invalid UTF-8: {e}")))
    }
}

/// Construct a `&str` from a raw `(ptr, len)` pair with UTF-8 validation.
///
/// SAFETY: caller must guarantee `ptr` points to `len` readable bytes.
pub unsafe fn raw_to_str<'a>(ptr: *const u8, len: usize) -> std::result::Result<&'a str, TantivyBindingError> {
    if ptr.is_null() {
        return Ok("");
    }
    let bytes = std::slice::from_raw_parts(ptr, len);
    std::str::from_utf8(bytes)
        .map_err(|e| TantivyBindingError::InvalidArgument(format!("invalid UTF-8: {e}")))
}

/// Discriminator for `Value`. Keep numerically stable — C++ side switches on
/// the integer values.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ValueTag {
    None = 0,
    Ptr = 1,
}

/// Owned `Vec<u32>` flattened to a (ptr, len, cap) triple. The `u32` width
/// matches Roaring bitmap's element type so the C++ side can do
/// `roaring->addMany(arr.len, arr.ptr)` without intermediate casting. Must
/// be released via `tantivy_free_u32_array` (which reconstructs the `Vec`
/// and drops it).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RustU32Array {
    pub ptr: *mut u32,
    pub len: usize,
    pub cap: usize,
}

impl RustU32Array {
    pub const EMPTY: RustU32Array = RustU32Array {
        ptr: std::ptr::null_mut(),
        len: 0,
        cap: 0,
    };

    /// Leak a `Vec<u32>` into a C-friendly triple. Caller MUST release via
    /// `tantivy_free_u32_array`.
    pub fn from_vec(mut v: Vec<u32>) -> Self {
        v.shrink_to_fit();
        let ptr = v.as_mut_ptr();
        let len = v.len();
        let cap = v.capacity();
        std::mem::forget(v);
        Self { ptr, len, cap }
    }

    /// SAFETY: caller must guarantee this `RustU32Array` was produced by
    /// `from_vec` and has not been freed.
    pub unsafe fn into_vec(self) -> Vec<u32> {
        if self.ptr.is_null() {
            Vec::new()
        } else {
            Vec::from_raw_parts(self.ptr, self.len, self.cap)
        }
    }
}

/// Tagged value carried inside `RustResult`. Only `Ptr` (a `*mut c_void`
/// handle) and `None` are used today; if a future FFI needs to return an
/// owned array or scalar, add a new variant rather than reviving the older
/// `Array` / `U64` tags. The C++ side already switches on `ValueTag`, so
/// adding new variants is a localized change.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Value {
    pub tag: ValueTag,
    pub ptr: *mut c_void,
}

impl Value {
    pub const NONE: Value = Value {
        tag: ValueTag::None,
        ptr: std::ptr::null_mut(),
    };

    pub fn from_ptr(p: *mut c_void) -> Self {
        Self { tag: ValueTag::Ptr, ptr: p }
    }
}

/// Owned array of NUL-terminated C strings. Must be released via
/// `tantivy_free_string_array`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RustStringArray {
    pub ptr: *mut *mut c_char,
    pub len: usize,
}

impl RustStringArray {
    pub const EMPTY: RustStringArray = RustStringArray {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    /// Leak the supplied strings as an array of `CString::into_raw` pointers.
    ///
    /// The backing buffer is hardened against allocator capacity ≠ length: we
    /// collect into a `Vec`, then shed any over-allocated capacity by going
    /// through `into_boxed_slice()` so the leaked pointer always corresponds
    /// to a slice of exactly `len` elements. The free path reconstructs the
    /// shape via `Box::from_raw(slice::from_raw_parts_mut(ptr, len))`, so the
    /// allocator gets back exactly what it handed out — no `Vec::from_raw_parts(ptr, len, len)`
    /// over an over-allocated `Vec` (which is UB on jemalloc-style allocators
    /// that round small `with_capacity(n)` up).
    pub fn from_strings(strings: Vec<String>) -> Self {
        if strings.is_empty() {
            return Self::EMPTY;
        }
        let ptrs: Vec<*mut c_char> = strings
            .into_iter()
            .map(|s| {
                CString::new(s)
                    .unwrap_or_else(|_| CString::new("").unwrap())
                    .into_raw()
            })
            .collect();
        let boxed: Box<[*mut c_char]> = ptrs.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut *mut c_char;
        Self { ptr, len }
    }
}

/// Result handed across the FFI for any fallible call.
///
/// On success: `success = true`, `value` carries data per its `tag`, `error`
/// is null. On failure: `success = false`, `value.tag = None`, `error` points
/// to a heap-allocated NUL-terminated UTF-8 string. **Always** call
/// `free_rust_result` exactly once on every result the FFI returned to you.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RustResult {
    pub success: bool,
    pub value: Value,
    pub error: *const c_char,
}

impl RustResult {
    pub fn ok_none() -> Self {
        Self { success: true, value: Value::NONE, error: std::ptr::null() }
    }

    pub fn ok_ptr(p: *mut c_void) -> Self {
        Self { success: true, value: Value::from_ptr(p), error: std::ptr::null() }
    }

    /// Construct a failure result. The message is leaked (CString::into_raw)
    /// and reclaimed by `free_rust_result`.
    pub fn err(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        // CString::new fails if msg contains an interior NUL; sanitize.
        let cstr = CString::new(msg.replace('\0', "?")).unwrap_or_else(|_| {
            CString::new("error message contained interior NUL").unwrap()
        });
        let raw = cstr.into_raw();
        Self { success: false, value: Value::NONE, error: raw as *const c_char }
    }
}
