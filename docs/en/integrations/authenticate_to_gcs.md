---
displayed_sidebar: "English"
---

# Authenticate to Google Cloud Storage

## Authentication methods

From v3.0 onwards, StarRocks supports using one of the following authentication methods to access Google Cloud Storage (GCS):

- VM-based authentication

  Use the credential attached to Google Cloud Compute Engine to authenticate GCS.

- Service account-based authentication

  Use a service account to authenticate GCS.

- Impersonation-based authentication

  Make a service account or virtual machine (VM) instance impersonate another service account.

## Scenarios

StarRocks can authenticate to GCS in the following scenarios:

- Batch load data from GCS.
- Back up data from and restore data to GCS.
- Query Parquet and ORC files in GCS.
- Query [Hive](../data_source/catalog/hive_catalog.md), [Iceberg](../data_source/catalog/iceberg_catalog.md), [Hudi](../data_source/catalog/hudi_catalog.md), and [Delta Lake](../data_source/catalog/deltalake_catalog.md) tables in GCS.

In this topic, [Hive catalog](../data_source/catalog/hive_catalog.md), [file external table](../data_source/file_external_table.md), and [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) are used as examples to show how StarRocks integrates with GCS in different scenarios. For information about `StorageCredentialParams` in the examples, see the "[Parameters](../integrations/authenticate_to_gcs.md#parameters)" section of this topic.

> **NOTE**
>
> StarRocks supports loading data or directly querying files from GCS only according to the gs protocol. Therefore, when you load data or query files from GCS, you must include `gs` as a prefix in the file path.

### External catalog

Use the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement to create a Hive catalog named `hive_catalog_gcs` as follows, in order to query files from GCS:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_gcs
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    StorageCredentialParams
);
```

### File external table

Use the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement to create a file external table named `external_table_gcs` as follows, in order to query a data file named `test_file_external_tbl` from GCS without any metastore:

```SQL
CREATE EXTERNAL TABLE external_table_gcs
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "gs:////test-gcs/test_file_external_tbl",
    "format" = "ORC",
    StorageCredentialParams
);
```

### Broker load

Use the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement to create a Broker Load job whose label is `test_db.label000`, in order to batch load data from GCS into the StarRocks table `target_table`:

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("gs://bucket_gcs/test_brokerload_ingestion/*")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    StorageCredentialParams
);
```

## Parameters

`StorageCredentialParams` represents a parameter set that describes how to authenticate to GCS with different authentication methods.

### VM-based authentication

If your StarRocks cluster is deployed on a VM instance hosted on Google Cloud Platform (GCP) and you want to use that VM instance to authenticate GCS, configure `StorageCredentialParams` as follows:

```Plain
"gcp.gcs.use_compute_engine_service_account" = "true"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
| ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
| gcp.gcs.use_compute_engine_service_account | false             | true                  | Specifies whether to directly use the service account that is bound to your Compute Engine. |

### Service account-based authentication

If you directly use a service account to authenticate GCS, configure `StorageCredentialParams` as follows:

```Plain
"gcp.gcs.service_account_email" = "<google_service_account_email>",
"gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
"gcp.gcs.service_account_private_key" = "<google_service_private_key>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| **Parameter**                          | **Default value** | **Value** **example**                                       | **Description**                                              |
| -------------------------------------- | ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| gcp.gcs.service_account_email          | ""                | "`user@hello.iam.gserviceaccount.com`"                        | The email address in the JSON file generated at the creation of the service account. |
| gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                  | The private key ID in the JSON file generated at the creation of the service account. |
| gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | The private key in the JSON file generated at the creation of the service account. |

### Impersonation-based authentication

#### Make a VM instance impersonate a service account

If your StarRocks cluster is deployed on a VM instance hosted on GCP and you want to make that VM instance impersonate a service account, so as to make StarRocks inherit the privileges from the service account to access GCS, configure `StorageCredentialParams` as follows:

```Plain
"gcp.gcs.use_compute_engine_service_account" = "true",
"gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
| ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
| gcp.gcs.use_compute_engine_service_account | false             | true                  | Specifies whether to directly use the service account that is bound to your Compute Engine. |
| gcp.gcs.impersonation_service_account      | ""                | "hello"               | The service account that you want to impersonate.            |

#### Make a service account impersonate another service account

If you want to make a service account (temporarily named as meta service account) impersonate another service account (temporarily named as data service account) and make StarRocks inherit the privileges from the data service account to access GCS, configure `StorageCredentialParams` as follows:

```Plain
"gcp.gcs.service_account_email" = "<google_service_account_email>",
"gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
"gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
"gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| **Parameter**                          | **Default value** | **Value** **example**                                       | **Description**                                              |
| -------------------------------------- | ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| gcp.gcs.service_account_email          | ""                | "`user@hello.iam.gserviceaccount.com`"                        | The email address in the JSON file generated at the creation of the meta service account. |
| gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                  | The private key ID in the JSON file generated at the creation of the meta service account. |
| gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | The private key in the JSON file generated at the creation of the meta service account. |
| gcp.gcs.impersonation_service_account  | ""                | "hello"                                                     | The data service account that you want to impersonate.       |
