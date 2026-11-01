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

fn view(s: &str) -> SrLanceString {
    SrLanceString {
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
unsafe fn reader(uri: &str, version: u64, fields: &[&str]) -> *mut SrLanceReader {
    let cols: Vec<_> = fields.iter().map(|s| view(s)).collect();
    let mut out = ptr::null_mut();
    let mut err = ptr::null_mut();
    let status = sr_lance_reader_open(
        view(uri),
        version,
        cols.as_ptr(),
        cols.len(),
        2,
        0,
        ptr::null(),
        0,
        SrLanceCancellation {
            check: None,
            context: ptr::null_mut(),
        },
        &mut out,
        &mut err,
    );
    assert_eq!(
        status,
        SR_LANCE_NEXT_BATCH,
        "{}",
        if err.is_null() {
            ""
        } else {
            CStr::from_ptr(err).to_str().unwrap()
        }
    );
    out
}
unsafe fn batches(reader: *mut SrLanceReader) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    loop {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        let mut err = ptr::null_mut();
        match sr_lance_reader_next(reader, &mut array, &mut schema, &mut err) {
            SR_LANCE_NEXT_EOF => break,
            SR_LANCE_NEXT_PENDING => continue,
            SR_LANCE_NEXT_BATCH => {
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
        assert_eq!(sr_lance_reader_version(r), 1);
        write(uri, vec![99], WriteMode::Append);
        let result = batches(r);
        sr_lance_reader_close(r);
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
        assert_eq!(sr_lance_reader_version(latest), 2);
        assert_eq!(
            batches(latest)
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            8
        );
        sr_lance_reader_close(latest);
        let old = reader(uri, 1, &["label"]);
        let rows = batches(old);
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 7);
        assert_eq!(
            rows.iter().map(|b| b.column(0).null_count()).sum::<usize>(),
            3
        );
        sr_lance_reader_close(old);
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
        sr_lance_reader_close(r);
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
            sr_lance_reader_open(
                view("unused"),
                0,
                ptr::null(),
                1,
                2,
                0,
                ptr::null(),
                0,
                SrLanceCancellation {
                    check: None,
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            SR_LANCE_ERROR
        );
        assert!(out.is_null());
        assert!(!error.is_null());
        sr_lance_free_error(error);
        assert_eq!(
            sr_lance_reader_open(
                view("unused"),
                0,
                ptr::null(),
                0,
                0,
                0,
                ptr::null(),
                0,
                SrLanceCancellation {
                    check: None,
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            SR_LANCE_ERROR
        );
        sr_lance_free_error(error);
        assert_eq!(
            boundary(&mut error, || panic!("test panic")),
            SR_LANCE_ERROR
        );
        assert_eq!(
            CStr::from_ptr(error).to_str().unwrap(),
            "Lance reader panicked"
        );
        sr_lance_free_error(error);
        assert_eq!(
            sr_lance_reader_open(
                view("/missing/private?sig=secret"),
                0,
                ptr::null(),
                0,
                2,
                0,
                ptr::null(),
                0,
                SrLanceCancellation {
                    check: None,
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            SR_LANCE_ERROR
        );
        let msg = CStr::from_ptr(error).to_str().unwrap();
        assert!(!msg.contains("secret"));
        assert!(!msg.contains("private"));
        sr_lance_free_error(error);
        sr_lance_reader_close(ptr::null_mut());
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

fn azure_workload_identity_cloud(suffix: &str) -> HashMap<String, String> {
    [
        (
            "fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
        ),
        ("fs.azure.account.oauth2.client.id", "workload-client"),
        ("fs.azure.account.oauth2.msi.tenant", "workload-tenant"),
        (
            "fs.azure.account.oauth2.token.file",
            "/var/run/secrets/azure/tokens/azure-identity-token",
        ),
    ]
    .into_iter()
    .map(|(key, value)| (format!("{key}{suffix}"), value.into()))
    .collect()
}

#[test]
fn azure_workload_identity_uses_scoped_or_global_projected_credentials() {
    let uri = "abfss://data@one.dfs.core.windows.net/test.lance";
    for suffix in ["", ".one.dfs.core.windows.net"] {
        let mut cloud = azure_workload_identity_cloud(suffix);
        if !suffix.is_empty() {
            // Account-specific values must take precedence over global defaults.
            for (key, _) in azure_workload_identity_cloud("") {
                cloud.insert(key, "unused-global-value".into());
            }
        }
        let opts = storage::options(uri, 2, &cloud).unwrap();
        assert_eq!(
            opts,
            HashMap::from([
                ("azure_storage_account_name".into(), "one".into()),
                ("azure_storage_client_id".into(), "workload-client".into()),
                ("azure_storage_tenant_id".into(), "workload-tenant".into()),
                (
                    "azure_federated_token_file".into(),
                    "/var/run/secrets/azure/tokens/azure-identity-token".into(),
                ),
            ])
        );
    }
    assert!(storage::options(
        uri,
        2,
        &azure_workload_identity_cloud(".two.dfs.core.windows.net")
    )
    .is_err());
}

#[test]
fn azure_workload_identity_rejects_incomplete_credentials() {
    let uri = "abfss://data@one.dfs.core.windows.net/test.lance";
    for suffix in ["", ".one.dfs.core.windows.net"] {
        for key in [
            "fs.azure.account.oauth2.client.id",
            "fs.azure.account.oauth2.msi.tenant",
            "fs.azure.account.oauth2.token.file",
        ] {
            for value in [None, Some(""), Some(" ")] {
                let mut cloud = azure_workload_identity_cloud(suffix);
                let key = format!("{key}{suffix}");
                if let Some(value) = value {
                    cloud.insert(key, value.into());
                } else {
                    cloud.remove(&key);
                }
                assert!(storage::options(uri, 2, &cloud).is_err());
            }
        }
    }
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
            sr_lance_reader_open(
                view("/must-not-be-opened"),
                0,
                ptr::null(),
                0,
                2,
                0,
                ptr::null(),
                0,
                SrLanceCancellation {
                    check: Some(cancelled),
                    context: ptr::null_mut()
                },
                &mut out,
                &mut error
            ),
            SR_LANCE_ERROR
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
        sr_lance_reader_close(r);
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

#[test]
fn unsigned_64_dictionary_export_preserves_values_keys_and_nulls() {
    use arrow_array::{types::Int32Type, ArrayRef, DictionaryArray, StructArray};

    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)]),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
    )
    .unwrap();
    let field = Arc::new(Field::new("values", dictionary.data_type().clone(), true));
    let nested = StructArray::from(vec![(
        field.clone(),
        Arc::new(dictionary.clone()) as ArrayRef,
    )]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            field.as_ref().clone(),
            Field::new("nested", nested.data_type().clone(), true),
        ])),
        vec![Arc::new(dictionary), Arc::new(nested)],
    )
    .unwrap();
    let normalized = normalize_batch(batch).unwrap();
    let nested = normalized
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    for column in [normalized.column(0), nested.column(0)] {
        let dictionary = column
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(
            dictionary.keys(),
            &Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)])
        );
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.data_type(), &DataType::Decimal128(20, 0));
        assert_eq!(values.value(0), i128::from(u64::MAX));
        assert_eq!(values.value(1), 0);
        assert!(values.is_null(2));
    }
}

#[test]
fn azure_blob_oauth_native_properties() {
    let uri = "wasbs://data@account.blob.core.windows.net/test.lance";
    let mut cloud = HashMap::from([("azure.blob.oauth2_client_id".into(), "identity".into())]);
    let opts = storage::options(uri, 2, &cloud).unwrap();
    assert_eq!(opts["azure_storage_account_name"], "account");
    assert_eq!(opts["azure_storage_client_id"], "identity");
    assert!(!opts.contains_key("azure_storage_client_secret"));
    assert!(!opts.contains_key("azure_storage_tenant_id"));
    cloud.insert("azure.blob.oauth2_client_secret".into(), "secret".into());
    assert!(storage::options(uri, 2, &cloud).is_err());
    cloud.insert("azure.blob.oauth2_tenant_id".into(), "tenant".into());
    let opts = storage::options(uri, 2, &cloud).unwrap();
    assert_eq!(opts["azure_storage_client_id"], "identity");
    assert_eq!(opts["azure_storage_client_secret"], "secret");
    assert_eq!(opts["azure_storage_tenant_id"], "tenant");
    cloud.remove("azure.blob.oauth2_client_secret");
    assert!(storage::options(uri, 2, &cloud).is_err());
    cloud.remove("azure.blob.oauth2_client_id");
    assert!(storage::options(uri, 2, &cloud).is_err());
}

#[test]
fn azure_blob_native_shared_key_and_sas_properties() {
    let uri = "wasbs://data@account.blob.core.windows.net/test.lance";
    for (property, value, option, expected) in [
        (
            "azure.blob.shared_key",
            "shared-key",
            "azure_storage_account_key",
            "shared-key",
        ),
        (
            "azure.blob.sas_token",
            "sig=test",
            "azure_storage_sas_key",
            "sig=test",
        ),
        (
            "azure.blob.sas_token",
            "?sig=test",
            "azure_storage_sas_key",
            "sig=test",
        ),
    ] {
        let cloud = HashMap::from([(property.into(), value.into())]);
        let opts = storage::options(uri, 2, &cloud).unwrap();
        assert_eq!(
            opts,
            HashMap::from([
                ("azure_storage_account_name".into(), "account".into()),
                (option.into(), expected.into()),
            ])
        );
    }
}

#[test]
fn azure_blob_hadoop_credentials_take_precedence_over_native_aliases() {
    let uri = "wasbs://data@account.blob.core.windows.net/test.lance";
    for (native, hadoop, option) in [
        (
            "azure.blob.shared_key",
            "fs.azure.account.key.account.blob.core.windows.net",
            "azure_storage_account_key",
        ),
        (
            "azure.blob.sas_token",
            "fs.azure.sas.data.account.blob.core.windows.net",
            "azure_storage_sas_key",
        ),
    ] {
        let cloud = HashMap::from([
            (native.into(), "native".into()),
            (hadoop.into(), "scoped".into()),
        ]);
        assert_eq!(storage::options(uri, 2, &cloud).unwrap()[option], "scoped");
    }
    let cloud = HashMap::from([
        ("azure.blob.shared_key".into(), "key".into()),
        ("azure.blob.sas_token".into(), "sas".into()),
    ]);
    let opts = storage::options(uri, 2, &cloud).unwrap();
    assert_eq!(opts["azure_storage_account_key"], "key");
    assert!(!opts.contains_key("azure_storage_sas_key"));
}
