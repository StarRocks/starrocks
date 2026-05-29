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

//! C ABI shim for the reader side.
//!
//! every query returns matching BE row ids (u32) via an
//! out-param `RustU32Array` to keep the data path compatible with C++
//! `roaring::Roaring::addMany(len, ptr)`. The function-level `RustResult`
//! still carries success / error_msg.
//!
//!   - tantivy_load_index_reader(path, field_name) -> RustResult(ptr)
//!   - tantivy_term_query(reader, term, *out)
//!   - tantivy_match_query(reader, terms[], count, *out)
//!   - tantivy_match_all_query(reader, terms[], count, *out)
//!   - tantivy_phrase_match_query(reader, terms[], count, slop, *out)
//!   - tantivy_wildcard_query(reader, pattern, *out)
//!   - tantivy_free_index_reader(reader)
//!
//! On the C++ side the typical use is:
//!
//! ```text
//! RustU32Array arr{};
//! RustResult rc = tantivy_term_query(r, "foo", &arr);
//! DeferOp release([&]{ tantivy_free_u32_array(arr); free_rust_result(rc); });
//! RETURN_IF_FFI_ERR(rc);
//! bitmap.addMany(arr.len, arr.ptr);
//! ```

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::path::PathBuf;

use tantivy::ReloadPolicy;

use crate::error::Result;
use crate::ffi::catch::catch_ffi;
use crate::ffi::handle::{as_ref, create_binding, free_binding};
use crate::ffi::result::{FFISlice, RustResult, RustU32Array, raw_to_str};
use crate::safe::pull_directory::PullDirectory;
use crate::safe::IndexReaderWrapper;

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

/// Read a `count`-sized array of `FFISlice` into owned `String`s.
unsafe fn read_terms(
    terms_ptr: *const FFISlice,
    count: usize,
) -> std::result::Result<Vec<String>, String> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if terms_ptr.is_null() {
        return Err("terms_ptr is NULL".to_string());
    }
    let slices = std::slice::from_raw_parts(terms_ptr, count);
    let mut out = Vec::with_capacity(count);
    for (i, s) in slices.iter().enumerate() {
        match s.as_str() {
            Ok(str_val) => out.push(str_val.to_owned()),
            Err(e) => return Err(format!("terms_ptr[{i}]: {e}")),
        }
    }
    Ok(out)
}

/// Shared boilerplate for `match_*` / `phrase_match` query FFI entry points:
/// validates `out`, dereferences the reader handle, decodes the term slice,
/// runs `query_fn`, and writes the row id array back into `*out`. The 3 query
/// functions only differ in `query_fn` (which wrapper method to call, and
/// whether to capture extra args like `slop`).
unsafe fn with_query_terms<F>(
    reader: *const c_void,
    terms: *const FFISlice,
    count: usize,
    out: *mut RustU32Array,
    query_fn: F,
) -> RustResult
where
    F: FnOnce(&IndexReaderWrapper, &[&str]) -> Result<Vec<u32>>,
{
    if out.is_null() {
        return RustResult::err("out pointer is NULL");
    }
    *out = RustU32Array::EMPTY;
    let r: &IndexReaderWrapper = match as_ref(reader) {
        Some(r) => r,
        None => return RustResult::err("reader is NULL"),
    };
    let owned = match read_terms(terms, count) {
        Ok(v) => v,
        Err(e) => return RustResult::err(e),
    };
    let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
    match query_fn(r, &refs) {
        Ok(ids) => {
            *out = RustU32Array::from_vec(ids);
            RustResult::ok_none()
        }
        Err(e) => RustResult::err(e.to_string()),
    }
}

/// Open an existing tantivy index at `path` and return a reader handle.
/// `field_name` must match the field used at write time.
///
/// SAFETY: `path` and `field_name` must be valid NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn tantivy_load_index_reader(
    path: *const c_char,
    field_name: *const c_char,
    tokenizer_name: *const c_char,
) -> RustResult {
    catch_ffi(|| {
        let path_str = cstr_or_err!(path, "path");
        let field_name_str = cstr_or_err!(field_name, "field_name");
        let tokenizer_str = cstr_or_err!(tokenizer_name, "tokenizer_name");
        match IndexReaderWrapper::load(std::path::Path::new(path_str), field_name_str, tokenizer_str) {
            Ok(r) => RustResult::ok_ptr(create_binding(r)),
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

#[derive(serde::Deserialize)]
struct FileTableEntry {
    offset: u64,
    length: u64,
}

/// Open an index from a compound `.idx` file via PullDirectory.
///
/// `ra_file_handle` is a C++ `RandomAccessFile*` (opaque pointer).
/// `file_table_json` is a NUL-terminated JSON string mapping filename to
/// `{"offset": u64, "length": u64}`.
/// `field_name` is the tantivy text field name.
///
/// Returns a `IndexReaderWrapper*` in `RustResult.value.ptr`. The returned
/// handle is interchangeable with handles from `tantivy_load_index_reader`:
/// callers consume it via `tantivy_term_query` / `tantivy_match_query` /
/// `tantivy_match_all_query` / `tantivy_phrase_match_query` and release it
/// via `tantivy_free_index_reader`.
///
/// SAFETY: `ra_file_handle` must be a valid pointer whose lifetime exceeds
/// the returned reader. `file_table_json` and `field_name` must be valid
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn tantivy_open_compound_reader(
    ra_file_handle: *mut c_void,
    file_table_json: *const c_char,
    field_name: *const c_char,
    tokenizer_name: *const c_char,
) -> RustResult {
    catch_ffi(|| {
        if ra_file_handle.is_null() {
            return RustResult::err("ra_file_handle is NULL");
        }
        let json_str = cstr_or_err!(file_table_json, "file_table_json");
        let field_name_str = cstr_or_err!(field_name, "field_name");
        let tokenizer_str = cstr_or_err!(tokenizer_name, "tokenizer_name");

        let parsed: HashMap<String, FileTableEntry> = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(e) => return RustResult::err(format!("failed to parse file_table_json: {e}")),
        };

        let file_table: HashMap<PathBuf, (u64, u64)> = parsed
            .into_iter()
            .map(|(name, entry)| (PathBuf::from(name), (entry.offset, entry.length)))
            .collect();

        let dir = PullDirectory::new(ra_file_handle, file_table);
        match IndexReaderWrapper::open(dir, field_name_str, tokenizer_str, ReloadPolicy::Manual) {
            Ok(reader) => RustResult::ok_ptr(create_binding(reader)),
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

/// Single-term query. Matching row ids are written into `*out`. Caller MUST
/// release `*out` via `tantivy_free_u32_array`.
///
/// SAFETY: `reader` and `out` must be non-NULL; `term` must be NUL-terminated.
#[no_mangle]
pub unsafe extern "C" fn tantivy_term_query(
    reader: *const c_void,
    term_ptr: *const u8,
    term_len: usize,
    out: *mut RustU32Array,
) -> RustResult {
    catch_ffi(|| {
        if out.is_null() {
            return RustResult::err("out pointer is NULL");
        }
        *out = RustU32Array::EMPTY;
        let r: &IndexReaderWrapper = match as_ref(reader) {
            Some(r) => r,
            None => return RustResult::err("reader is NULL"),
        };
        let term_str = match raw_to_str(term_ptr, term_len) {
            Ok(s) => s,
            Err(e) => return RustResult::err(format!("term: {e}")),
        };
        match r.term_query(term_str) {
            Ok(ids) => {
                *out = RustU32Array::from_vec(ids);
                RustResult::ok_none()
            }
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

/// MATCH_ANY query: returns rows matching ANY of `terms`.
///
/// SAFETY: `reader`, `out` non-NULL; `terms` is a `count`-array of NUL-
/// terminated C strings (or `count == 0`).
#[no_mangle]
pub unsafe extern "C" fn tantivy_match_query(
    reader: *const c_void,
    terms: *const FFISlice,
    count: usize,
    out: *mut RustU32Array,
) -> RustResult {
    catch_ffi(|| with_query_terms(reader, terms, count, out, |r, t| r.match_any_query(t)))
}

/// MATCH_ALL query: returns rows matching ALL of `terms`.
///
/// SAFETY: same as `tantivy_match_query`.
#[no_mangle]
pub unsafe extern "C" fn tantivy_match_all_query(
    reader: *const c_void,
    terms: *const FFISlice,
    count: usize,
    out: *mut RustU32Array,
) -> RustResult {
    catch_ffi(|| with_query_terms(reader, terms, count, out, |r, t| r.match_all_query(t)))
}

/// MATCH_PHRASE query: returns rows where `terms` appear in order with at
/// most `slop` positional gaps.
///
/// SAFETY: same as `tantivy_match_query`.
#[no_mangle]
pub unsafe extern "C" fn tantivy_phrase_match_query(
    reader: *const c_void,
    terms: *const FFISlice,
    count: usize,
    slop: u32,
    out: *mut RustU32Array,
) -> RustResult {
    catch_ffi(|| with_query_terms(reader, terms, count, out, |r, t| r.phrase_query(t, slop)))
}

/// MATCH_WILDCARD query: returns rows whose indexed term matches the SQL
/// `LIKE` / `MATCH` pattern. `%` and `*` are equivalent multi-char wildcards
///
/// SAFETY: `reader` and `out` must be non-NULL; `pattern_ptr` may be NULL
/// only when `pattern_len == 0`.
#[no_mangle]
pub unsafe extern "C" fn tantivy_wildcard_query(
    reader: *const c_void,
    pattern_ptr: *const u8,
    pattern_len: usize,
    out: *mut RustU32Array,
) -> RustResult {
    catch_ffi(|| {
        if out.is_null() {
            return RustResult::err("out pointer is NULL");
        }
        *out = RustU32Array::EMPTY;
        let r: &IndexReaderWrapper = match as_ref(reader) {
            Some(r) => r,
            None => return RustResult::err("reader is NULL"),
        };
        let pattern = match raw_to_str(pattern_ptr, pattern_len) {
            Ok(s) => s,
            Err(e) => return RustResult::err(format!("pattern: {e}")),
        };
        match r.wildcard_query(pattern) {
            Ok(ids) => {
                *out = RustU32Array::from_vec(ids);
                RustResult::ok_none()
            }
            Err(e) => RustResult::err(e.to_string()),
        }
    })
}

/// Release a reader handle. Safe on NULL.
///
/// SAFETY: `reader` must be NULL or have been returned by
/// `tantivy_load_index_reader` and not previously freed.
#[no_mangle]
pub unsafe extern "C" fn tantivy_free_index_reader(reader: *mut c_void) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        free_binding::<IndexReaderWrapper>(reader);
    }));
}
