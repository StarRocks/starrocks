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
use std::sync::{Arc, OnceLock};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, Float32Array, RecordBatch, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use futures::stream::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::session::Session;
use lance_linalg::distance::MetricType;
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;

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

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SrLanceVectorOptions {
    vector_column: SrLanceString,
    metric_type: SrLanceString,
    query_vector: *const SrLanceString,
    query_vector_len: usize,
    limit_k: i64,
    index_segment_uuids: *const SrLanceString,
    index_segment_uuid_count: usize,
    nprobes: i32,
    refine_factor: i32,
    ef: i32,
    query_parallelism: i32,
}

pub struct SrLanceReader {
    stream: DatasetRecordBatchStream,
}

struct VectorOptions {
    vector_column: String,
    metric_type: MetricType,
    query_vector: Vec<f32>,
    limit_k: usize,
    index_segment_uuids: Vec<Uuid>,
    nprobes: Option<usize>,
    refine_factor: Option<u32>,
    ef: Option<usize>,
    query_parallelism: Option<i32>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CacheConfig {
    index_cache_size_bytes: usize,
    metadata_cache_size_bytes: usize,
}

struct SharedSession {
    config: CacheConfig,
    session: Arc<Session>,
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

fn cache_size_from_i64(name: &str, value: i64) -> Result<usize, String> {
    if value < 0 {
        return Err(format!("{name} must be non-negative, got {value}"));
    }
    usize::try_from(value).map_err(|_| format!("{name} is too large: {value}"))
}

fn session(
    index_cache_size_bytes: i64,
    metadata_cache_size_bytes: i64,
) -> Result<Arc<Session>, String> {
    static SESSION: OnceLock<SharedSession> = OnceLock::new();
    let config = CacheConfig {
        index_cache_size_bytes: cache_size_from_i64(
            "lance_index_cache_size_bytes",
            index_cache_size_bytes,
        )?,
        metadata_cache_size_bytes: cache_size_from_i64(
            "lance_metadata_cache_size_bytes",
            metadata_cache_size_bytes,
        )?,
    };
    let shared = SESSION.get_or_init(|| SharedSession {
        config,
        session: Arc::new(Session::new(
            config.index_cache_size_bytes,
            config.metadata_cache_size_bytes,
            Default::default(),
        )),
    });
    if shared.config != config {
        return Err(format!(
            "Lance cache config changed after session initialization: initial index={} metadata={}, current index={} metadata={}",
            shared.config.index_cache_size_bytes,
            shared.config.metadata_cache_size_bytes,
            config.index_cache_size_bytes,
            config.metadata_cache_size_bytes
        ));
    }
    Ok(shared.session.clone())
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

fn parse_query_vector(values: Vec<String>) -> Result<Vec<f32>, String> {
    values
        .into_iter()
        .map(|value| {
            let parsed = value
                .parse::<f32>()
                .map_err(|e| format!("invalid Lance query vector element '{value}': {e}"))?;
            if !parsed.is_finite() {
                return Err(format!(
                    "invalid Lance query vector element '{value}': must be finite"
                ));
            }
            Ok(parsed)
        })
        .collect()
}

fn parse_metric_type(value: &str) -> Result<MetricType, String> {
    match value.to_ascii_lowercase().as_str() {
        "l2" | "euclidean" => Ok(MetricType::L2),
        "cosine" | "cosine_distance" => Ok(MetricType::Cosine),
        "dot" | "inner_product" => Ok(MetricType::Dot),
        _ => Err(format!("unsupported Lance vector metric type '{value}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metric_type() {
        assert_eq!(parse_metric_type("l2").unwrap(), MetricType::L2);
        assert_eq!(parse_metric_type("cosine").unwrap(), MetricType::Cosine);
        assert_eq!(
            parse_metric_type("cosine_distance").unwrap(),
            MetricType::Cosine
        );
        assert_eq!(parse_metric_type("dot").unwrap(), MetricType::Dot);
        assert_eq!(parse_metric_type("inner_product").unwrap(), MetricType::Dot);
        assert!(parse_metric_type("hamming").is_err());
    }
}

unsafe fn vector_options_from_raw(
    value: *const SrLanceVectorOptions,
) -> Result<Option<VectorOptions>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let value = *value;
    let vector_column = string_from_raw(value.vector_column)?;
    if vector_column.is_empty() {
        return Err("Lance vector column must not be empty".to_string());
    }
    let metric_type = string_from_raw(value.metric_type)?;
    let metric_type = parse_metric_type(&metric_type)?;
    if value.limit_k <= 0 {
        return Err(format!(
            "invalid Lance vector search limit {}",
            value.limit_k
        ));
    }
    let query_vector = strings_from_raw(value.query_vector, value.query_vector_len)?;
    if query_vector.is_empty() {
        return Err("Lance query vector must not be empty".to_string());
    }
    let query_vector = parse_query_vector(query_vector)?;

    let index_segment_uuids =
        strings_from_raw(value.index_segment_uuids, value.index_segment_uuid_count)?;
    if index_segment_uuids.is_empty() {
        return Err("Lance vector search requires at least one index segment".to_string());
    }
    let index_segment_uuids = index_segment_uuids
        .into_iter()
        .map(|value| {
            Uuid::parse_str(&value)
                .map_err(|e| format!("invalid Lance index segment UUID '{value}': {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(VectorOptions {
        vector_column,
        metric_type,
        query_vector,
        limit_k: value.limit_k as usize,
        index_segment_uuids,
        nprobes: positive_option(value.nprobes).map(|v| v as usize),
        refine_factor: positive_option(value.refine_factor).map(|v| v as u32),
        ef: positive_option(value.ef).map(|v| v as usize),
        query_parallelism: query_parallelism_option(value.query_parallelism),
    }))
}

fn positive_option(value: i32) -> Option<i32> {
    if value > 0 {
        Some(value)
    } else {
        None
    }
}

fn query_parallelism_option(value: i32) -> Option<i32> {
    if value >= -1 {
        Some(value)
    } else {
        None
    }
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
    vector_options: Option<VectorOptions>,
    index_cache_size_bytes: i64,
    metadata_cache_size_bytes: i64,
) -> Result<SrLanceReader, String> {
    let dataset = DatasetBuilder::from_uri(&dataset_uri)
        .with_storage_options(storage_options)
        .with_session(session(index_cache_size_bytes, metadata_cache_size_bytes)?)
        .load()
        .await
        .map_err(|e| format!("failed to open Lance dataset {dataset_uri}: {e}"))?;
    if let Some(vector_options) = vector_options {
        let query_vector = Float32Array::from(vector_options.query_vector);
        let mut scanner = dataset.scan();
        scanner
            .nearest(
                &vector_options.vector_column,
                &query_vector,
                vector_options.limit_k,
            )
            .map_err(|e| format!("failed to set Lance vector search: {e}"))?;
        scanner.distance_metric(vector_options.metric_type);
        scanner.use_index(true);
        if let Some(nprobes) = vector_options.nprobes {
            scanner.nprobes(nprobes);
        }
        if let Some(refine_factor) = vector_options.refine_factor {
            scanner.refine(refine_factor);
        }
        if let Some(ef) = vector_options.ef {
            scanner.ef(ef);
        }
        if let Some(query_parallelism) = vector_options.query_parallelism {
            scanner.query_parallelism(query_parallelism);
        }
        scanner
            .with_index_segments(vector_options.index_segment_uuids)
            .map_err(|e| format!("failed to set Lance vector index segments: {e}"))?;
        let mut projected_columns = columns;
        if !projected_columns.iter().any(|column| column == "_distance") {
            projected_columns.push("_distance".to_string());
        }
        scanner.project(&projected_columns).map_err(|e| {
            format!(
                "failed to project Lance columns {:?}: {e}",
                projected_columns
            )
        })?;
        if batch_size > 0 {
            scanner.batch_size(batch_size as usize);
        }
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| format!("failed to create Lance vector scan stream: {e}"))?;
        return Ok(SrLanceReader { stream });
    }

    if fragment_id < 0 {
        return Err(format!("invalid negative Lance fragment id {fragment_id}"));
    }
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
    vector_options: *const SrLanceVectorOptions,
    index_cache_size_bytes: i64,
    metadata_cache_size_bytes: i64,
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
        let vector_options = vector_options_from_raw(vector_options)?;
        let reader = runtime().block_on(open_reader(
            dataset_uri,
            fragment_id,
            columns,
            batch_size,
            storage_options,
            vector_options,
            index_cache_size_bytes,
            metadata_cache_size_bytes,
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
