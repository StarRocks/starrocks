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

use std::collections::HashMap;
use std::ffi::{c_char, c_int, CString};
use std::ptr;
use std::slice;
use std::sync::OnceLock;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use futures::stream::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::DatasetRecordBatchStream;
use tokio::runtime::{Builder, Runtime};

const SR_LANCE_NEXT_EOF: c_int = 0;
const SR_LANCE_NEXT_BATCH: c_int = 1;
const SR_LANCE_ERROR: c_int = -1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SrLanceString {
    data: *const c_char,
    len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SrLanceStringPair {
    key: SrLanceString,
    value: SrLanceString,
}

pub struct SrLanceReader {
    stream: DatasetRecordBatchStream,
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("starrocks-lance-rs")
            .build()
            .expect("failed to initialize Lance Rust runtime")
    })
}

unsafe fn string_from_raw(value: SrLanceString) -> Result<String, String> {
    if value.data.is_null() {
        return Err("null string pointer".to_string());
    }
    let bytes = slice::from_raw_parts(value.data as *const u8, value.len);
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|e| format!("invalid utf8 string: {e}"))
}

unsafe fn strings_from_raw(
    values: *const SrLanceString,
    len: usize,
) -> Result<Vec<String>, String> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if values.is_null() {
        return Err("null string array pointer".to_string());
    }
    let values = slice::from_raw_parts(values, len);
    values.iter().map(|value| string_from_raw(*value)).collect()
}

unsafe fn storage_options_from_raw(
    values: *const SrLanceStringPair,
    len: usize,
) -> Result<HashMap<String, String>, String> {
    let mut options = HashMap::with_capacity(len);
    if len == 0 {
        return Ok(options);
    }
    if values.is_null() {
        return Err("null storage option array pointer".to_string());
    }
    for pair in slice::from_raw_parts(values, len) {
        options.insert(string_from_raw(pair.key)?, string_from_raw(pair.value)?);
    }
    Ok(options)
}

fn set_error(error: *mut *mut c_char, message: impl Into<String>) {
    if error.is_null() {
        return;
    }
    let sanitized = message.into().replace('\0', "\\0");
    let c_string =
        CString::new(sanitized).unwrap_or_else(|_| CString::new("unknown error").unwrap());
    unsafe {
        *error = c_string.into_raw();
    }
}

async fn open_reader(
    dataset_uri: String,
    fragment_id: i32,
    columns: Vec<String>,
    batch_size: i32,
    storage_options: HashMap<String, String>,
) -> Result<SrLanceReader, String> {
    if fragment_id < 0 {
        return Err(format!("invalid negative Lance fragment id {fragment_id}"));
    }
    let dataset = DatasetBuilder::from_uri(&dataset_uri)
        .with_storage_options(storage_options)
        .load()
        .await
        .map_err(|e| format!("failed to open Lance dataset {dataset_uri}: {e}"))?;
    let fragment = dataset.get_fragment(fragment_id as usize).ok_or_else(|| {
        format!("Lance fragment {fragment_id} not found in dataset {dataset_uri}")
    })?;
    let mut scanner = fragment.scan();
    if columns.is_empty() {
        scanner
            .empty_project()
            .map_err(|e| format!("failed to apply empty projection: {e}"))?;
    } else {
        scanner
            .project(&columns)
            .map_err(|e| format!("failed to project Lance columns {:?}: {e}", columns))?;
    }
    if batch_size > 0 {
        scanner.batch_size(batch_size as usize);
    }
    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| format!("failed to create Lance scan stream: {e}"))?;
    Ok(SrLanceReader { stream })
}

#[no_mangle]
pub unsafe extern "C" fn sr_lance_reader_open(
    dataset_uri: SrLanceString,
    fragment_id: i32,
    columns: *const SrLanceString,
    column_count: usize,
    batch_size: i32,
    storage_options: *const SrLanceStringPair,
    storage_option_count: usize,
    out_reader: *mut *mut SrLanceReader,
    error: *mut *mut c_char,
) -> c_int {
    if out_reader.is_null() {
        set_error(error, "null output reader pointer");
        return SR_LANCE_ERROR;
    }
    *out_reader = ptr::null_mut();

    let result = (|| -> Result<*mut SrLanceReader, String> {
        let dataset_uri = string_from_raw(dataset_uri)?;
        let columns = strings_from_raw(columns, column_count)?;
        let storage_options = storage_options_from_raw(storage_options, storage_option_count)?;
        let reader = runtime().block_on(open_reader(
            dataset_uri,
            fragment_id,
            columns,
            batch_size,
            storage_options,
        ))?;
        Ok(Box::into_raw(Box::new(reader)))
    })();

    match result {
        Ok(reader) => {
            *out_reader = reader;
            SR_LANCE_NEXT_BATCH
        }
        Err(e) => {
            set_error(error, e);
            SR_LANCE_ERROR
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn sr_lance_reader_next(
    reader: *mut SrLanceReader,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    out_rows: *mut i64,
    error: *mut *mut c_char,
) -> c_int {
    if reader.is_null() {
        set_error(error, "null Lance reader pointer");
        return SR_LANCE_ERROR;
    }
    if out_array.is_null() || out_schema.is_null() || out_rows.is_null() {
        set_error(error, "null Arrow output pointer");
        return SR_LANCE_ERROR;
    }

    let reader = &mut *reader;
    let next = runtime().block_on(reader.stream.next());
    match next {
        None => SR_LANCE_NEXT_EOF,
        Some(Err(e)) => {
            set_error(error, format!("failed to read next Lance batch: {e}"));
            SR_LANCE_ERROR
        }
        Some(Ok(batch)) => export_batch(batch, out_array, out_schema, out_rows, error),
    }
}

unsafe fn export_batch(
    batch: RecordBatch,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    out_rows: *mut i64,
    error: *mut *mut c_char,
) -> c_int {
    let row_count = batch.num_rows() as i64;
    let schema = match FFI_ArrowSchema::try_from(batch.schema().as_ref()) {
        Ok(schema) => schema,
        Err(e) => {
            set_error(error, format!("failed to export Arrow schema: {e}"));
            return SR_LANCE_ERROR;
        }
    };
    let struct_array = StructArray::from(batch);
    let array = FFI_ArrowArray::new(&struct_array.to_data());
    ptr::write(out_array, array);
    ptr::write(out_schema, schema);
    *out_rows = row_count;
    SR_LANCE_NEXT_BATCH
}

#[no_mangle]
pub unsafe extern "C" fn sr_lance_reader_close(reader: *mut SrLanceReader) {
    if !reader.is_null() {
        drop(Box::from_raw(reader));
    }
}

#[no_mangle]
pub unsafe extern "C" fn sr_lance_free_error(error: *mut c_char) {
    if !error.is_null() {
        drop(CString::from_raw(error));
    }
}
