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

//! Lifecycle helpers — release ownership of values handed to C++.

use std::ffi::{c_char, CString};

use crate::ffi::result::{RustF32Array, RustResult, RustStringArray, RustU32Array};

/// Release a `RustResult` and any owned content it carries.
///
/// - On failure result, releases the heap-allocated error string.
/// - `Ptr` variant carries an opaque handle whose lifetime is managed
///   separately (via the dedicated free helpers); `None` carries no memory.
///
/// SAFETY: `result` must be a value previously produced by this crate. Calling
/// this twice on the same logical result is undefined behavior.
#[no_mangle]
pub unsafe extern "C" fn free_rust_result(result: RustResult) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if !result.error.is_null() {
            // CString::from_raw takes *mut, so cast away const. We own this
            // string because we leaked it from CString::into_raw.
            drop(CString::from_raw(result.error as *mut c_char));
        }
    }));
}

/// Release a `RustU32Array` produced by a query FFI. Safe on a NULL/empty
/// array (treated as a no-op).
///
/// SAFETY: `array` must be a value previously produced by `RustU32Array::from_vec`.
#[no_mangle]
pub unsafe extern "C" fn tantivy_free_u32_array(array: RustU32Array) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(array.into_vec());
    }));
}

/// Release a `RustF32Array` produced by a scored query FFI. Safe on a
/// NULL/empty array (treated as a no-op).
///
/// SAFETY: `array` must be a value previously produced by `RustF32Array::from_vec`.
#[no_mangle]
pub unsafe extern "C" fn tantivy_free_f32_array(array: RustF32Array) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(array.into_vec());
    }));
}

/// Release a `RustStringArray` produced by `RustStringArray::from_strings`.
///
/// SAFETY: `array` must have been produced by `RustStringArray::from_strings`
/// and not previously freed.
#[no_mangle]
pub unsafe extern "C" fn tantivy_free_string_array(array: RustStringArray) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if array.ptr.is_null() || array.len == 0 {
            return;
        }
        let slice: *mut [*mut c_char] = std::slice::from_raw_parts_mut(array.ptr, array.len);
        let boxed: Box<[*mut c_char]> = Box::from_raw(slice);
        for p in boxed.iter() {
            if !p.is_null() {
                drop(CString::from_raw(*p));
            }
        }
    }));
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_tokenize(
    tokenizer_name: *const c_char,
    text_ptr: *const u8,
    text_len: usize,
    out: *mut RustStringArray,
) -> RustResult {
    use crate::ffi::catch::catch_ffi;
    use crate::ffi::result::raw_to_str;
    catch_ffi(|| {
        if out.is_null() {
            return RustResult::err("out pointer is NULL");
        }
        *out = RustStringArray::EMPTY;
        if tokenizer_name.is_null() {
            return RustResult::err("tokenizer_name is NULL");
        }
        let tok_str = match std::ffi::CStr::from_ptr(tokenizer_name).to_str() {
            Ok(s) => s,
            Err(e) => return RustResult::err(format!("tokenizer_name is not valid UTF-8: {e}")),
        };
        let text_str = match raw_to_str(text_ptr, text_len) {
            Ok(s) => s,
            Err(e) => return RustResult::err(format!("text: {e}")),
        };
        match crate::safe::tokenizer::tokenize(tok_str, text_str) {
            Ok(tokens) => {
                *out = RustStringArray::from_strings(tokens);
                RustResult::ok_none()
            }
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}
