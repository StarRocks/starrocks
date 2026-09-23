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

//! Lance-only C ABI. Arrow buffers retain their Rust release callbacks after reader close.
//! Handles are thread-confined; calls on one handle must not overlap.
mod storage;

// Rust I/O runs on Tokio threads and its buffers are released on BE threads.
// Keep allocations out of BE's thread-local malloc hooks; charge exported
// buffers explicitly in the C++ adapter instead of freeing against the wrong query.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::OnceLock;
use std::time::Duration;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Schema};
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::DatasetRecordBatchStream;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

const ERROR: c_int = -1;
const EOF: c_int = 0;
const BATCH: c_int = 1;
const PENDING: c_int = 2;
type Result<T> = std::result::Result<T, &'static str>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LanceString {
    data: *const c_char,
    len: usize,
}

#[repr(C)]
pub struct LanceProperty {
    key: LanceString,
    value: LanceString,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LanceCancellation {
    check: Option<unsafe extern "C" fn(*mut c_void) -> bool>,
    context: *mut c_void,
}

pub struct LanceReader {
    stream: DatasetRecordBatchStream,
    version: u64,
    exhausted: bool,
}

fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<Result<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("lance-reader")
                .build()
                .map_err(|_| "Cannot initialize Lance runtime")
        })
        .as_ref()
        .map_err(|e| *e)
}

unsafe fn items<'a, T>(data: *const T, len: usize) -> Result<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() || len > isize::MAX as usize / std::mem::size_of::<T>() {
        return Err("Invalid Lance FFI buffer");
    }
    Ok(slice::from_raw_parts(data, len))
}

unsafe fn string(value: LanceString) -> Result<String> {
    std::str::from_utf8(items(value.data.cast::<u8>(), value.len)?)
        .map(str::to_owned)
        .map_err(|_| "Invalid UTF-8 in Lance argument")
}

// Never propagate SDK error text: object-store errors can contain SAS tokens,
// credentials or signed URLs. Stable messages identify the failed operation.
unsafe fn boundary(error: *mut *mut c_char, f: impl FnOnce() -> Result<c_int>) -> c_int {
    if !error.is_null() {
        *error = ptr::null_mut();
    }
    let result = catch_unwind(AssertUnwindSafe(f)).unwrap_or(Err("Lance reader panicked"));
    match result {
        Ok(code) => code,
        Err(message) => {
            if !error.is_null() {
                *error = CString::new(message)
                    .expect("static error has no NUL")
                    .into_raw();
            }
            ERROR
        }
    }
}

async fn open(
    uri: String,
    version: u64,
    columns: Vec<String>,
    batch_size: usize,
    options: HashMap<String, String>,
) -> Result<LanceReader> {
    // Each dataset owns its session. Do not cache object stores across credentials.
    let mut builder = DatasetBuilder::from_uri(&uri)
        .with_storage_options(options)
        .with_index_cache_size_bytes(0)
        .with_metadata_cache_size_bytes(8 * 1024 * 1024);
    if version != 0 {
        builder = builder.with_version(version);
    }
    let dataset = builder
        .load()
        .await
        .map_err(|_| "Cannot open Lance dataset (check URI, version and credentials)")?;
    let version = dataset.version().version;
    let mut scan = dataset.scan();
    if columns.is_empty() {
        scan.empty_project()
            .map_err(|_| "Cannot create empty Lance projection")?;
    } else {
        scan.project(&columns)
            .map_err(|_| "Cannot project Lance columns")?;
    }
    scan.batch_size(batch_size);
    scan.batch_readahead(1);
    scan.fragment_readahead(1);
    // Full-dataset scan, including every fragment. Do not push a limit ahead of residual filters.
    let stream = scan
        .try_into_stream()
        .await
        .map_err(|_| "Cannot create Lance scan stream")?;
    Ok(LanceReader {
        stream,
        version,
        exhausted: false,
    })
}

/// All input buffers are borrowed for this call only; output owns the returned handle.
/// `version == 0` opens latest once; subsequent batches retain that snapshot.
/// # Safety
/// Pointer/length pairs must address valid buffers; out_reader and error must be writable.
#[no_mangle]
pub unsafe extern "C" fn sr_lance_open(
    uri: LanceString,
    version: u64,
    columns: *const LanceString,
    column_count: usize,
    batch_size: i32,
    cloud_type: i32,
    properties: *const LanceProperty,
    property_count: usize,
    cancellation: LanceCancellation,
    out_reader: *mut *mut LanceReader,
    error: *mut *mut c_char,
) -> c_int {
    boundary(error, || {
        if out_reader.is_null() {
            return Err("Missing Lance reader output");
        }
        *out_reader = ptr::null_mut();
        if batch_size <= 0 {
            return Err("Lance batch size must be positive");
        }
        let uri = string(uri)?;
        let columns = items(columns, column_count)?
            .iter()
            .map(|v| string(*v))
            .collect::<Result<Vec<_>>>()?;
        let mut cloud = HashMap::new();
        for prop in items(properties, property_count)? {
            let value = string(prop.value)?;
            if !value.is_empty() {
                cloud.insert(string(prop.key)?, value);
            }
        }
        let options = storage::options(&uri, cloud_type, &cloud)?;
        let reader = runtime()?.block_on(async {
            let future = open(uri, version, columns, batch_size as usize, options);
            let mut future = std::pin::pin!(future);
            loop {
                if cancellation
                    .check
                    .is_some_and(|check| check(cancellation.context))
                {
                    return Err("Lance scan cancelled");
                }
                // Keep the same future between polls; cancellation drops it and its resources.
                if let Ok(result) =
                    tokio::time::timeout(Duration::from_millis(100), &mut future).await
                {
                    break result;
                }
            }
        })?;
        *out_reader = Box::into_raw(Box::new(reader));
        Ok(BATCH)
    })
}

/// Return 1 for a batch, 0 for EOF, 2 to poll again, or -1 for an error.
/// Polling bounds cancellation latency without losing the stream between calls.
/// # Safety
/// Reader must be live, outputs writable and Arrow outputs released before reuse.
#[no_mangle]
pub unsafe extern "C" fn sr_lance_next(
    reader: *mut LanceReader,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> c_int {
    boundary(error, || {
        if reader.is_null() || array.is_null() || schema.is_null() {
            return Err("Invalid Lance next argument");
        }
        let reader = &mut *reader;
        if reader.exhausted {
            return Ok(EOF);
        }
        let result = runtime()?.block_on(async {
            tokio::time::timeout(Duration::from_millis(100), reader.stream.next()).await
        });
        let batch = match result {
            Err(_) => return Ok(PENDING),
            Ok(None) => {
                reader.exhausted = true;
                return Ok(EOF);
            }
            Ok(Some(Err(_))) => {
                reader.exhausted = true;
                return Err("Cannot read Lance batch");
            }
            Ok(Some(Ok(batch))) => batch,
        };
        export(batch, array, schema)?;
        Ok(BATCH)
    })
}

// FE maps UInt64 to DECIMAL(20,0). The generic BE Arrow converter otherwise
// falls back through signed BIGINT, losing the upper half of the UInt64 range.
// Normalize recursively inside Lance rather than changing other connectors.
fn export_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::List(field) => DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(export_type(field.data_type())),
        )),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(export_type(field.data_type())),
        )),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(export_type(field.data_type())),
            ),
            *size,
        ),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(export_type(field.data_type()))
                })
                .collect(),
        ),
        DataType::Map(field, sorted) => DataType::Map(
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(export_type(field.data_type())),
            ),
            *sorted,
        ),
        _ => data_type.clone(),
    }
}

fn normalize_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            field
                .as_ref()
                .clone()
                .with_data_type(export_type(field.data_type()))
        })
        .collect::<Vec<_>>();
    if fields
        .iter()
        .zip(batch.schema().fields())
        .all(|(a, b)| a.data_type() == b.data_type())
    {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(&fields)
        .map(|(column, field)| {
            arrow_cast::cast(column, field.data_type())
                .map_err(|_| "Cannot widen Lance unsigned integers")
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(|_| "Cannot normalize Lance batch")
}

unsafe fn export(
    batch: RecordBatch,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<()> {
    let batch = normalize_batch(batch)?;
    let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())
        .map_err(|_| "Cannot export Lance Arrow schema")?;
    let ffi_array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
    ptr::write(array, ffi_array);
    ptr::write(schema, ffi_schema);
    Ok(())
}

/// # Safety
/// Reader must be null or a live handle, with no simultaneous operations.
#[no_mangle]
pub unsafe extern "C" fn sr_lance_version(reader: *const LanceReader) -> u64 {
    if reader.is_null() {
        0
    } else {
        (*reader).version
    }
}

/// # Safety
/// Reader must be null or a live handle; it must not be used again after close.
#[no_mangle]
pub unsafe extern "C" fn sr_lance_close(reader: *mut LanceReader) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !reader.is_null() {
            // Stream cleanup may require a Tokio context, even on a BE worker thread.
            if let Ok(rt) = runtime() {
                let _guard = rt.enter();
                drop(Box::from_raw(reader));
            } else {
                drop(Box::from_raw(reader));
            }
        }
    }));
}

/// # Safety
/// Error must be null or an unreleased error returned by this library.
#[no_mangle]
pub unsafe extern "C" fn sr_lance_free_error(error: *mut c_char) {
    if !error.is_null() {
        drop(CString::from_raw(error));
    }
}

#[cfg(test)]
mod tests;
