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

//! C ABI shim for the writer side.
//!
//! Surface:
//!   - tantivy_create_index_writer(path, field_name, tokenizer)
//!   - tantivy_index_add_strings_batch(writer, values_ptr, count)
//!   - tantivy_commit_index(writer)
//!   - tantivy_free_index_writer(writer)
//!
//! Documents are added in row order; null rows MUST be sent as empty strings
//! so tantivy DocId remains aligned with the BE row id. The BE side keeps a
//! separate Roaring null bitmap and serializes it before pack time.

use std::ffi::{c_char, c_void, CStr};

use crate::ffi::catch::catch_ffi;
use crate::ffi::handle::{as_mut, create_binding, free_binding};
use crate::ffi::result::{FFISlice, RustResult};
use crate::safe::IndexWriterWrapper;

/// SAFETY helper: turn a NUL-terminated C string into a `&str`. Returns
/// a `RustResult` failure if the pointer is null or not valid UTF-8.
macro_rules! cstr_or_err {
    ($ptr:expr, $what:expr) => {{
        if $ptr.is_null() {
            return RustResult::err(concat!($what, " pointer is NULL"));
        }
        match CStr::from_ptr($ptr).to_str() {
            Ok(s) => s,
            Err(e) => return RustResult::err(format!("{} is not valid UTF-8: {e}", $what)),
        }
    }};
}

/// Create a fresh tantivy index at `path` with one TEXT field named
/// `field_name` and the analyzer chain identified by `tokenizer`. Returns an
/// opaque writer handle in `RustResult.value.ptr`. Caller MUST release with
/// `tantivy_free_index_writer` and the result with `free_rust_result`.
///
/// SAFETY: `path`, `field_name`, `tokenizer` must be valid NUL-terminated
/// C strings.
#[no_mangle]
pub unsafe extern "C" fn tantivy_create_index_writer(
    path: *const c_char,
    field_name: *const c_char,
    tokenizer: *const c_char,
) -> RustResult {
    catch_ffi(|| {
        let path_str = cstr_or_err!(path, "path");
        let field_name_str = cstr_or_err!(field_name, "field_name");
        let tokenizer_str = cstr_or_err!(tokenizer, "tokenizer");
        match IndexWriterWrapper::create(
            std::path::Path::new(path_str),
            field_name_str,
            tokenizer_str,
        ) {
            Ok(w) => RustResult::ok_ptr(create_binding(w)),
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

/// Append a batch of UTF-8 strings as documents in order. `values_ptr` is an
/// array of `count` `FFISlice` structs. A slice with NULL ptr is treated as
/// an empty placeholder doc so doc-id alignment is preserved across null rows.
///
/// SAFETY: `writer` must be a non-NULL writer handle; `values_ptr` must be a
/// non-NULL array of `count` `FFISlice` structs (or `count == 0`).
#[no_mangle]
pub unsafe extern "C" fn tantivy_index_add_strings_batch(
    writer: *mut c_void,
    values_ptr: *const FFISlice,
    count: usize,
) -> RustResult {
    catch_ffi(|| {
        let w: &mut IndexWriterWrapper = match as_mut(writer) {
            Some(w) => w,
            None => return RustResult::err("writer is NULL"),
        };
        if count == 0 {
            return RustResult::ok_none();
        }
        if values_ptr.is_null() {
            return RustResult::err("values_ptr is NULL");
        }

        let slices = std::slice::from_raw_parts(values_ptr, count);
        let mut refs: Vec<&str> = Vec::with_capacity(count);
        for (i, s) in slices.iter().enumerate() {
            match s.as_str() {
                Ok(str_val) => refs.push(str_val),
                Err(e) => {
                    return RustResult::err(format!("values_ptr[{i}]: {e}"))
                }
            }
        }
        match w.add_strings_batch(&refs) {
            Ok(()) => RustResult::ok_none(),
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

/// Flush queued docs to disk. After success, a freshly loaded reader will
/// observe the new docs.
///
/// SAFETY: same as `tantivy_index_add_strings_batch`.
#[no_mangle]
pub unsafe extern "C" fn tantivy_commit_index(writer: *mut c_void) -> RustResult {
    catch_ffi(|| {
        let w: &mut IndexWriterWrapper = match as_mut(writer) {
            Some(w) => w,
            None => return RustResult::err("writer is NULL"),
        };
        match w.commit() {
            Ok(()) => RustResult::ok_none(),
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

/// Release a writer handle. Safe on NULL.
///
/// SAFETY: `writer` must be NULL or have been returned by
/// `tantivy_create_index_writer` and not previously freed.
#[no_mangle]
pub unsafe extern "C" fn tantivy_free_index_writer(writer: *mut c_void) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        free_binding::<IndexWriterWrapper>(writer);
    }));
}
