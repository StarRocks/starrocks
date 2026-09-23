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

use super::*;
use arrow_array::{
    Decimal128Array, Int32Array, ListArray, RecordBatchIterator, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use lance::{
    dataset::{WriteMode, WriteParams},
    Dataset,
};
use std::ffi::CStr;
use std::sync::Arc;

fn view(s: &str) -> LanceString {
    LanceString {
        data: s.as_ptr().cast(),
        len: s.len(),
    }
}
fn write(uri: &str, values: Vec<i32>, mode: WriteMode) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("u64", DataType::UInt64, true),
    ]));
    let labels = StringArray::from(
        values
            .iter()
            .map(|x| if x % 2 == 0 { Some("even") } else { None })
            .collect::<Vec<_>>(),
    );
    let wide = UInt64Array::from(
        values
            .iter()
            .map(|v| u64::MAX - *v as u64)
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(values)),
            Arc::new(labels),
            Arc::new(wide),
        ],
    )
    .unwrap();
    let input = RecordBatchIterator::new(vec![Ok(batch)], schema);
    runtime()
        .unwrap()
        .block_on(Dataset::write(
            input,
            uri,
            Some(WriteParams {
                mode,
                max_rows_per_file: 2,
                ..Default::default()
            }),
        ))
        .unwrap();
}
unsafe fn reader(uri: &str, version: u64, fields: &[&str]) -> *mut LanceReader {
    let cols: Vec<_> = fields.iter().map(|s| view(s)).collect();
    let mut out = ptr::null_mut();
    let mut err = ptr::null_mut();
    let status = sr_lance_open(
        view(uri),
        version,
        cols.as_ptr(),
        cols.len(),
        2,
        0,
        ptr::null(),
        0,
        LanceCancellation {
            check: None,
            context: ptr::null_mut(),
        },
        &mut out,
        &mut err,
    );
    assert_eq!(
        status,
        BATCH,
        "{}",
        if err.is_null() {
            ""
        } else {
            CStr::from_ptr(err).to_str().unwrap()
        }
    );
    out
}
unsafe fn batches(reader: *mut LanceReader) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    loop {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        let mut err = ptr::null_mut();
        match sr_lance_next(reader, &mut array, &mut schema, &mut err) {
            EOF => break,
            PENDING => continue,
            BATCH => {
                let data = arrow_array::ffi::from_ffi(array, &schema).unwrap();
                out.push(RecordBatch::from(StructArray::from(data)));
            }
            _ => panic!("{}", CStr::from_ptr(err).to_str().unwrap()),
        }
    }
    out
}

#[test]
fn full_dataset_projection_snapshot_and_buffer_lifetime() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("rows.lance");
    let uri = uri.to_str().unwrap();
    write(uri, (0..7).collect(), WriteMode::Create);
    unsafe {
        let r = reader(uri, 0, &["id"]);
        assert_eq!(sr_lance_version(r), 1);
        write(uri, vec![99], WriteMode::Append);
        let result = batches(r);
        sr_lance_close(r);
        // Arrays remain readable after their scanner and dataset have been destroyed.
        let mut ids = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, (0..7).collect::<Vec<_>>());
        assert!(result
            .iter()
            .all(|b| b.num_columns() == 1 && b.num_rows() <= 2));
        let latest = reader(uri, 0, &["id", "label"]);
        assert_eq!(sr_lance_version(latest), 2);
        assert_eq!(
            batches(latest)
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            8
        );
        sr_lance_close(latest);
        let old = reader(uri, 1, &["label"]);
        let rows = batches(old);
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 7);
        assert_eq!(
            rows.iter().map(|b| b.column(0).null_count()).sum::<usize>(),
            3
        );
        sr_lance_close(old);
    }
}

#[test]
fn empty_projection_preserves_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("count.lance");
    let uri = uri.to_str().unwrap();
    write(uri, vec![1, 2, 3], WriteMode::Create);
    unsafe {
        let r = reader(uri, 0, &[]);
        let rows = batches(r);
        sr_lance_close(r);
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(rows.iter().all(|b| b.num_columns() == 0));
    }
}

#[test]
fn invalid_arguments_and_errors_do_not_escape_ffi() {
    unsafe {
        let mut out = ptr::null_mut();
        let mut error = ptr::null_mut();
        assert_eq!(
            sr_lance_open(
                view("unused"),
                0,
                ptr::null(),
                1,
                2,
                0,
                ptr::null(),
                0,
                LanceCancellation {
                    check: None,
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            ERROR
        );
        assert!(out.is_null());
        assert!(!error.is_null());
        sr_lance_free_error(error);
        assert_eq!(
            sr_lance_open(
                view("unused"),
                0,
                ptr::null(),
                0,
                0,
                0,
                ptr::null(),
                0,
                LanceCancellation {
                    check: None,
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            ERROR
        );
        sr_lance_free_error(error);
        assert_eq!(boundary(&mut error, || panic!("test panic")), ERROR);
        assert_eq!(
            CStr::from_ptr(error).to_str().unwrap(),
            "Lance reader panicked"
        );
        sr_lance_free_error(error);
        assert_eq!(
            sr_lance_open(
                view("/missing/private?sig=secret"),
                0,
                ptr::null(),
                0,
                2,
                0,
                ptr::null(),
                0,
                LanceCancellation {
                    check: None,
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            ERROR
        );
        let msg = CStr::from_ptr(error).to_str().unwrap();
        assert!(!msg.contains("secret"));
        assert!(!msg.contains("private"));
        sr_lance_free_error(error);
        sr_lance_close(ptr::null_mut());
        sr_lance_free_error(ptr::null_mut());
    }
}

#[test]
fn credentials_are_account_scoped_and_unsupported_auth_fails_closed() {
    let uri = "abfss://data@one.dfs.core.windows.net/test.lance";
    let cloud = HashMap::from([
        (
            "fs.azure.sas.fixed.token.one.dfs.core.windows.net".into(),
            "?sig=one".into(),
        ),
        (
            "fs.azure.sas.fixed.token.two.dfs.core.windows.net".into(),
            "?sig=two".into(),
        ),
    ]);
    let opts = storage::options(uri, 2, &cloud).unwrap();
    assert_eq!(opts["azure_storage_sas_key"], "sig=one");
    assert_eq!(opts["azure_storage_account_name"], "one");
    assert!(storage::options(
        "abfss://data@three.dfs.core.windows.net/test.lance",
        2,
        &cloud
    )
    .is_err());
    assert!(storage::options(
        "s3://bucket/data",
        1,
        &HashMap::from([("aws.s3.iam_role_arn".into(), "role".into())])
    )
    .is_err());
    assert!(storage::options(uri, 5, &cloud).is_err());
}

#[test]
fn temporary_s3_credentials_and_endpoints() {
    let cloud = HashMap::from([
        ("aws.s3.access_key".into(), "key".into()),
        ("aws.s3.secret_key".into(), "secret".into()),
        ("aws.s3.session_token".into(), "session".into()),
        ("aws.s3.endpoint".into(), "localhost:9000".into()),
        ("aws.s3.enable_ssl".into(), "false".into()),
        ("aws.s3.enable_path_style_access".into(), "true".into()),
    ]);
    let opts = storage::options("s3://bucket/test", 1, &cloud).unwrap();
    assert_eq!(opts["aws_session_token"], "session");
    assert_eq!(opts["aws_endpoint"], "http://localhost:9000");
    assert_eq!(opts["aws_allow_http"], "true");
    assert_eq!(opts["aws_virtual_hosted_style_request"], "false");
}

#[test]
fn create_be_integration_fixture() {
    if let Ok(uri) = std::env::var("LANCE_TEST_DATASET") {
        write(&uri, (0..7).collect(), WriteMode::Overwrite);
    }
}

#[test]
fn cancelled_open_returns_before_storage_access() {
    unsafe extern "C" fn cancelled(_: *mut std::ffi::c_void) -> bool {
        true
    }
    unsafe {
        let mut out = ptr::null_mut();
        let mut error = ptr::null_mut();
        assert_eq!(
            sr_lance_open(
                view("/must-not-be-opened"),
                0,
                ptr::null(),
                0,
                2,
                0,
                ptr::null(),
                0,
                LanceCancellation {
                    check: Some(cancelled),
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            ERROR
        );
        assert!(out.is_null());
        assert_eq!(
            CStr::from_ptr(error).to_str().unwrap(),
            "Lance scan cancelled"
        );
        sr_lance_free_error(error);
    }
}

#[test]
fn incomplete_explicit_credentials_do_not_fall_back_to_host_identity() {
    let cloud = HashMap::from([("aws.s3.access_key".into(), "key".into())]);
    assert!(storage::options("s3://bucket/test", 1, &cloud).is_err());
    let cloud = HashMap::from([(
        "fs.azure.account.oauth.provider.type".into(),
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider".into(),
    )]);
    assert_eq!(
        storage::options(
            "abfss://data@one.dfs.core.windows.net/test.lance",
            2,
            &cloud
        )
        .unwrap_err(),
        "Missing Azure OAuth client ID"
    );
}

#[test]
fn unsigned_64_export_preserves_full_range_and_nested_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("unsigned.lance");
    let uri = uri.to_str().unwrap();
    write(uri, vec![0], WriteMode::Create);
    unsafe {
        let r = reader(uri, 0, &["u64"]);
        let result = batches(r);
        sr_lance_close(r);
        let values = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.value(0), i128::from(u64::MAX));
    }
    let array = ListArray::from_iter_primitive::<arrow_array::types::UInt64Type, _, _>([
        Some(vec![Some(u64::MAX), None]),
        None,
    ]);
    let field = Field::new("vector", array.data_type().clone(), true);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)]).unwrap();
    let normalized = normalize_batch(batch).unwrap();
    let array = normalized
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = array.value(0);
    let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(values.value(0), i128::from(u64::MAX));
    assert!(values.is_null(1));
    assert!(array.is_null(1));
}
