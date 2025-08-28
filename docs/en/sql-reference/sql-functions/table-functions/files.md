---
displayed_sidebar: docs
---

# FILES



Defines data files in remote storage. It can be used to:

- [Load or query data from a remote storage system](#files-for-loading)
- [Unload data into a remote storage system](#files-for-unloading)

Currently, the FILES() function supports the following data sources and file formats:

- **Data sources:**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - Other S3-compatible storage system
  - Microsoft Azure Blob Storage
  - NFS(NAS)
- **File formats:**
  - Parquet
  - ORC (Supported from v3.3 onwards)
  - CSV (Supported from v3.3 onwards)
  - Avro (Supported from v3.4.4 onwards and for loading only)

From v3.2 onwards, FILES() further supports complex data types including ARRAY, JSON, MAP, and STRUCT in addition to basic data types.

## FILES() for loading

From v3.1.0 onwards, StarRocks supports defining read-only files in remote storage using the table function FILES(). It can access remote storage with the path-related properties of the files, infers the table schema of the data in the files, and returns the data rows. You can directly query the data rows using [SELECT](../../sql-statements/table_bucket_part_index/SELECT.md), load the data rows into an existing table using [INSERT](../../sql-statements/loading_unloading/INSERT.md), or create a new table and load the data rows into it using [CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md). From v3.3.4, you can also view the schema of a data file using FILES() with [DESC](../../sql-statements/table_bucket_part_index/DESCRIBE.md).

### Syntax

```SQL
FILES( data_location , [data_format] [, schema_detect ] [, StorageCredentialParams ] [, columns_from_path ] [, list_files_only ] [, list_recursively])
```

### Parameters

All parameters are in the `"key" = "value"` pairs.

#### data_location

The URI used to access the files.

You can specify a path or a file. For example, you can specify this parameter as `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"` to load a data file named `20210411` from the path `/user/data/tablename` on the HDFS server.

You can also specify this parameter as the save path of multiple data files by using wildcards `?`, `*`, `[]`, `{}`, or `^`. For example, you can specify this parameter as `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` or `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"` to load the data files from all partitions or only `202104` partitions in the path `/user/data/tablename` on the HDFS server.

:::note

Wildcards can also be used to specify intermediate paths.

:::

- To access HDFS, you need to specify this parameter as:

  ```SQL
  "path" = "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
  -- Example: "path" = "hdfs://127.0.0.1:9000/path/file.parquet"
  ```

- To access AWS S3:

  - If you use the S3 protocol, you need to specify this parameter as:

    ```SQL
    "path" = "s3://<s3_path>"
    -- Example: "path" = "s3://path/file.parquet"
    ```

  - If you use the S3A protocol, you need to specify this parameter as:

    ```SQL
    "path" = "s3a://<s3_path>"
    -- Example: "path" = "s3a://path/file.parquet"
    ```

- To access Google Cloud Storage, you need to specify this parameter as:

  ```SQL
  "path" = "s3a://<gcs_path>"
  -- Example: "path" = "s3a://path/file.parquet"
  ```

- To access Azure Blob Storage:

  - If your storage account allows access over HTTP, you need to specify this parameter as:

    ```SQL
    "path" = "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- Example: "path" = "wasb://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```
  
  - If your storage account allows access over HTTPS, you need to specify this parameter as:

    ```SQL
    "path" = "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- Example: "path" = "wasbs://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

- To access NFS(NAS):

  ```SQL
  "path" = "file:///<absolute_path>"
  -- Example: "path" = "file:///home/ubuntu/parquetfile/file.parquet"
  ```

  :::note

  To access the files in NFS via the `file://` protocol, you need to mount a NAS device as NFS under the same directory of each BE or CN node.

  :::

#### data_format

The format of the data file. Valid values:
- `parquet`
- `orc` (Supported from v3.3 onwards)
- `csv` (Supported from v3.3 onwards)
- `avro` (Supported from v3.4.4 onwards and for loading only)

You must set detailed options for specific data file formats.

When `list_files_only` is set to `true`, you do not need to specify `data_format`.

##### Parquet

Example of the Parquet format:

```SQL
"format"="parquet",
"parquet.use_legacy_encoding" = "true",   -- for unloading only
"parquet.version" = "2.6"                 -- for unloading only
```

###### parquet.use_legacy_encoding

Controls the encoding technique used for DATETIME and DECIMAL data types. Valid values: `true` and `false` (default). This property is only supported for data unloading.

If this item is set to `true`:

- For DATETIME type, the system uses `INT96` encoding.
- For DECIMAL type, the system uses `fixed_len_byte_array` encoding.

If this item is set to `false`:

- For DATETIME type, the system uses `INT64` encoding.
- For DECIMAL type, the system uses `INT32` or `INT64` encoding.

:::note

For DECIMAL 128 data type, only `fixed_len_byte_array` encoding is available. `parquet.use_legacy_encoding` does not take effect.

:::

###### parquet.version

Controls the Parquet version into which the system unloads data. Supported from v3.4.6 onwards. Valid values: `1.0`, `2.4`, and `2.6` (default). This property is only supported for data unloading.

##### CSV

Example for the CSV format:

```SQL
"format"="csv",
"csv.column_separator"="\\t",
"csv.enclose"='"',
"csv.skip_header"="1",
"csv.escape"="\\"
```

###### csv.column_separator

Specifies the column separator used when the data file is in CSV format. If you do not specify this parameter, this parameter defaults to `\\t`, indicating tab. The column separator you specify using this parameter must be the same as the column separator that is actually used in the data file. Otherwise, the load job will fail due to inadequate data quality.

Tasks that use Files() are submitted according to the MySQL protocol. StarRocks and MySQL both escape characters in the load requests. Therefore, if the column separator is an invisible character such as tab, you must add a backslash (`\`) preceding the column separator. For example, you must input `\\t` if the column separator is `\t`, and you must input `\\n` if the column separator is `\n`. Apache Hive™ files use `\x01` as their column separator, so you must input `\\x01` if the data file is from Hive.

> **NOTE**
>
> - For CSV data, you can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.
> - Null values are denoted by using `\N`. For example, a data file consists of three columns, and a record from that data file holds data in the first and third columns but no data in the second column. In this situation, you need to use `\N` in the second column to denote a null value. This means the record must be compiled as `a,\N,b` instead of `a,,b`. `a,,b` denotes that the second column of the record holds an empty string.

###### csv.enclose

Specifies the character that is used to wrap the field values in the data file according to RFC4180 when the data file is in CSV format. Type: single-byte character. Default value: `NONE`. The most prevalent characters are single quotation mark (`'`) and double quotation mark (`"`).

All special characters (including row separators and column separators) wrapped by using the `enclose`-specified character are considered normal symbols. StarRocks can do more than RFC4180 as it allows you to specify any single-byte character as the `enclose`-specified character.

If a field value contains an `enclose`-specified character, you can use the same character to escape that `enclose`-specified character. For example, you set `enclose` to `"`, and a field value is `a "quoted" c`. In this case, you can enter the field value as `"a ""quoted"" c"` into the data file.

###### csv.skip_header

Specifies the number of header rows to skip in the CSV-formatted data. Type: INTEGER. Default value: `0`.

In some CSV-formatted data files, a number of header rows are used to define metadata such as column names and column data types. By setting the `skip_header` parameter, you can enable StarRocks to skip these header rows. For example, if you set this parameter to `1`, StarRocks skips the first row of the data file during data loading.

The header rows in the data file must be separated by using the row separator that you specify in the load statement.

###### csv.escape

Specifies the character that is used to escape various special characters, such as row separators, column separators, escape characters, and `enclose`-specified characters, which are then considered by StarRocks to be common characters and are parsed as part of the field values in which they reside. Type: single-byte character. Default value: `NONE`. The most prevalent character is slash (`\`), which must be written as double slashes (`\\`) in SQL statements.

> **NOTE**
>
> The character specified by `escape` is applied to both inside and outside of each pair of `enclose`-specified characters.
> Two examples are as follows:
> - When you set `enclose` to `"` and `escape` to `\`, StarRocks parses `"say \"Hello world\""` into `say "Hello world"`.
> - Assume that the column separator is comma (`,`). When you set `escape` to `\`, StarRocks parses `a, b\, c` into two separate field values: `a` and `b, c`.

#### schema_detect

From v3.2 onwards, FILES() supports automatic schema detection and unionization of the same batch of data files. StarRocks first detects the schema of the data by sampling certain data rows of a random data file in the batch. Then, StarRocks unionizes the columns from all the data files in the batch.

You can configure the sampling rule using the following parameters:

- `auto_detect_sample_files`: the number of random data files to sample in each batch. By default, the first and last files are selected. Range: [0, + ∞]. Default: `2`.
- `auto_detect_sample_rows`: the number of data rows to scan in each sampled data file. Range: [0, + ∞]. Default: `500`.

After the sampling, StarRocks unionizes the columns from all the data files according to these rules:

- For columns with different column names or indices, each column is identified as an individual column, and, eventually, the union of all individual columns is returned.
- For columns with the same column name but different data types, they are identified as the same column but with a general data type on a relative fine granularity level. For example, if the column `col1` in file A is INT but DECIMAL in file B, DOUBLE is used in the returned column.
  - All integer columns will be unionized as an integer type on an overall rougher granularity level.
  - Integer columns together with FLOAT type columns will be unionized as the DECIMAL type.
  - String types are used for unionizing other types.
- Generally, the STRING type can be used to unionize all data types.

You can refer to Example 5.

If StarRocks fails to unionize all the columns, it generates a schema error report that includes the error information and all the file schemas.

> **CAUTION**
>
> All data files in a single batch must be of the same file format.

##### Push down target table schema check

From v3.4.0 onwards, the system supports pushing down the target table schema check to the Scan stage of FILES().

Schema detection of FILES() is not fully strict. For example, any integer column in CSV files is inferred and checked as the BIGINT type when the function is reading the files. In this case, if the corresponding column in the target table is the TINYINT type, the CSV data records that exceed the BIGINT type will not be filtered. Instead, they will be filled with NULL implicitly.

To address this issue, the system introduces the dynamic FE configuration item `files_enable_insert_push_down_schema` to control whether to push down the target table schema check to the Scan stage of FILES(). By setting `files_enable_insert_push_down_schema` to `true`, the system will filter the data records which fail the target table schema check at the file reading.

##### Union files with different schema

From v3.4.0 onwards, the system supports unionizing files with different schema, and by default, an error will be returned if there are non-existent columns. By setting the property `fill_mismatch_column_with` to `null`, you can allow the system to assign NULL values to the non-existent columns instead of returning an error.

`fill_mismatch_column_with`: The behavior of the system after a non-existent column is detected when unionizing files with different schema. Valid values:
- `none`: An error will be returned if a non-existent column is detected.
- `null`: NULL values will be assigned to the non-existent column.

For example, the files to read are from different partitions of a Hive table, and Schema Change has been performed on the newer partitions. When reading both new and old partitions, you can set `fill_mismatch_column_with` to `null`, and the system will unionize the schema of the new and old partition files, and assign NULL values to the non-existent columns.

The system unionizes the schema of Parquet and ORC files based on the column names, and that of CSV files based on the position (order) of the columns.

##### Infer STRUCT type from Parquet

From v3.4.0 onwards, FILES() supports inferring the STRUCT type data from Parquet files.

#### StorageCredentialParams

The authentication information used by StarRocks to access your storage system.

StarRocks currently supports accessing HDFS with the simple authentication, accessing AWS S3 and GCS with the IAM user-based authentication, and accessing Azure Blob Storage with Shared Key, SAS Token, Managed Identity, and Service Principal.

##### HDFS

- Use the simple authentication to access HDFS:

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **Key**                        | **Required** | **Description**                                              |
  | ------------------------------ | ------------ | ------------------------------------------------------------ |
  | hadoop.security.authentication | No           | The authentication method. Valid value: `simple` (Default). `simple` represents simple authentication, meaning no authentication. |
  | username                       | Yes          | The username of the account that you want to use to access the NameNode of the HDFS cluster. |
  | password                       | Yes          | The password of the account that you want to use to access the NameNode of the HDFS cluster. |

- Use the Kerberos authentication to access HDFS:

  Currently, FILES() supports Kerberos authentication with HDFS only via the configuration file **hdfs-site.xml** placed under the **fe/conf**, **be/conf**, and **cn/conf** directories.

  In addition, you need to append the following option in the configuration item `JAVA_OPTS` in each FE configuration file **fe.conf**, BE configuration file **be.conf**, and CN configuration file **cn.conf**:

  ```Plain
  # Specify the local path to which the Kerberos configuration file is stored.
  -Djava.security.krb5.conf=<path_to_kerberos_conf_file>
  ```

  Example:

  ```Properties
  JAVA_OPTS="-Xlog:gc*:${LOG_DIR}/be.gc.log.$DATE:time -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log -Djava.security.krb5.conf=/etc/krb5.conf"
  ```

  You also need to run the `kinit` command on each FE, BE, and CN node to obtain Ticket Granting Ticket (TGT) from Key Distribution Center (KDC).

  ```Bash
  kinit -kt <path_to_keytab_file> <principal>
  ```

  To run this command, the principal you use must have the write access to your HDFS cluster. In addition, you need to set a crontab for the command to schedule the task by a specific interval, thus preventing the authentication from expiring.

  Example:

  ```Bash
  # Renew TGT every 6 hours.
  0 */6 * * * kinit -kt sr.keytab sr/test.starrocks.com@STARROCKS.COM > /tmp/kinit.log
  ```

- Access HDFS with HA mode enabled:

  Currently, FILES() supports access to HDFS with HA mode enabled only via the configuration file **hdfs-site.xml** placed under the **fe/conf**, **be/conf**, and **cn/conf** directories.

##### AWS S3

If you choose AWS S3 as your storage system, take one of the following actions:

- To choose the instance profile-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- To choose the assumed role-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- To choose the IAM user-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | Specifies whether to enable the credential methods instance profile and assumed role. Valid values: `true` and `false`. Default value: `false`. |
| aws.s3.iam_role_arn         | No       | The ARN of the IAM role that has privileges on your AWS S3 bucket. If you choose assumed role as the credential method for accessing AWS S3, you must specify this parameter. |
| aws.s3.region               | Yes      | The region in which your AWS S3 bucket resides. Example: `us-west-1`. |
| aws.s3.access_key           | No       | The access key of your IAM user. If you choose IAM user as the credential method for accessing AWS S3, you must specify this parameter. |
| aws.s3.secret_key           | No       | The secret key of your IAM user. If you choose IAM user as the credential method for accessing AWS S3, you must specify this parameter. |

For information about how to choose an authentication method for accessing AWS S3 and how to configure an access control policy in AWS IAM Console, see [Authentication parameters for accessing AWS S3](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3).

##### Google GCS

If you choose Google GCS as your storage system, take one of the following actions:

- To choose the VM-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Specifies whether to directly use the service account that is bound to your Compute Engine. |

- To choose the service account-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | The email address in the JSON file generated at the creation of the service account. |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | The private key ID in the JSON file generated at the creation of the service account. |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | The private key in the JSON file generated at the creation of the service account. |

- To choose the impersonation-based authentication method, configure `StorageCredentialParams` as follows:

  - Make a VM instance impersonate a service account:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    The following table describes the parameters you need to configure in `StorageCredentialParams`.

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Specifies whether to directly use the service account that is bound to your Compute Engine. |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | The service account that you want to impersonate.            |

  - Make a service account (named as meta service account) impersonate another service account (named as data service account):

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    The following table describes the parameters you need to configure in `StorageCredentialParams`.

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | The email address in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | The private key ID in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | The private key in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | The data service account that you want to impersonate.       |

##### Azure Blob Storage

- Use Shared Key to access Azure Blob Storage:

  ```SQL
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.shared_key      | Yes          | The Shared Key that you can use to access the Azure Blob Storage account. |

- Use SAS Token to access Azure Blob Storage:

  ```SQL
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.sas_token       | Yes          | The SAS token that you can use to access the Azure Blob Storage account. |

- Use Managed Identity to access Azure Blob Storage (Supported from v3.4.4 onwards):

  :::note
  - Only User-assigned Managed Identities with Client ID credentials are supported.
  - The FE dynamic configuration `azure_use_native_sdk` (Default: `true`) controls whether to allow the system to use authentication with Managed Identity and Service Principals.
  :::

  ```SQL
  "azure.blob.oauth2_use_managed_identity" = "true",
  "azure.blob.oauth2_client_id" = "<oauth2_client_id>"
  ```

  | **Key**                                | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.oauth2_use_managed_identity | Yes          | Whether to use Managed Identity to access the Azure Blob Storage account. Set it to `true`.                  |
  | azure.blob.oauth2_client_id            | Yes          | The Client ID of the Managed Identity that you can use to access the Azure Blob Storage account.                |

- Use Service Principal to access Azure Blob Storage (Supported from v3.4.4 onwards):

  :::note
  - Only Client Secret credentials are supported.
  - The FE dynamic configuration `azure_use_native_sdk` (Default: `true`) controls whether to allow the system to use authentication with Managed Identity and Service Principals.
  :::

  ```SQL
  "azure.blob.oauth2_client_id" = "<oauth2_client_id>",
  "azure.blob.oauth2_client_secret" = "<oauth2_client_secret>",
  "azure.blob.oauth2_tenant_id" = "<oauth2_tenant_id>"
  ```

  | **Key**                                | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.oauth2_client_id            | Yes          | The Client ID of the Service Principal that you can use to access the Azure Blob Storage account.                    |
  | azure.blob.oauth2_client_secret        | Yes          | The Client Secret of the Service Principal that you can use to access the Azure Blob Storage account.          |
  | azure.blob.oauth2_tenant_id            | Yes          | The Tenant ID of the Service Principal that you can use to access the Azure Blob Storage account.                |

##### Azure Data Lake Storage Gen2

If you choose Data Lake Storage Gen2 as your storage system, take one of the following actions:

- To choose the Managed Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | Specifies whether to enable the Managed Identity authentication method. Set the value to `true`. |
  | azure.adls2.oauth2_tenant_id            | Yes          | The ID of the tenant whose data you want to access.          |
  | azure.adls2.oauth2_client_id            | Yes          | The client (application) ID of the managed identity.         |

- To choose the Shared Key authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | The username of your Data Lake Storage Gen2 storage account. |
  | azure.adls2.shared_key      | Yes          | The shared key of your Data Lake Storage Gen2 storage account. |

- To choose the Service Principal authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  The following table describes the parameters you need to configure `in StorageCredentialParams`.

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | The client (application) ID of the service principal.        |
  | azure.adls2.oauth2_client_secret   | Yes          | The value of the new client (application) secret created.    |
  | azure.adls2.oauth2_client_endpoint | Yes          | The OAuth 2.0 token endpoint (v1) of the service principal or application. |

##### Azure Data Lake Storage Gen1

If you choose Data Lake Storage Gen1 as your storage system, take one of the following actions:

- To choose the Managed Service Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | Specifies whether to enable the Managed Service Identity authentication method. Set the value to `true`. |

- To choose the Service Principal authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | The client (application) ID of the .                         |
  | azure.adls1.oauth2_credential | Yes          | The value of the new client (application) secret created.    |
  | azure.adls1.oauth2_endpoint   | Yes          | The OAuth 2.0 token endpoint (v1) of the service principal or application. |

##### Other S3-compatible storage system

If you choose other S3-compatible storage system, such as MinIO, configure `StorageCredentialParams` as follows:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | Specifies whether to enable SSL connection. Valid values: `true` and `false`. Default value: `true`. |
| aws.s3.enable_path_style_access  | Yes      | Specifies whether to enable path-style URL access. Valid values: `true` and `false`. Default value: `false`. For MinIO, you must set the value to `true`. |
| aws.s3.endpoint                  | Yes      | The endpoint that is used to connect to your S3-compatible storage system instead of AWS S3. |
| aws.s3.access_key                | Yes      | The access key of your IAM user. |
| aws.s3.secret_key                | Yes      | The secret key of your IAM user. |

#### columns_from_path

From v3.2 onwards, StarRocks can extract the value of a key/value pair from the file path as the value of a column.

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

Suppose the data file **file1** is stored under a path in the format of `/geo/country=US/city=LA/`. You can specify the `columns_from_path` parameter as `"columns_from_path" = "country, city"` to extract the geographic information in the file path as the value of columns that are returned. For further instructions, see Example 4.

#### list_files_only

From v3.4.0 onwards, FILES() supports only list the files when reading them.

```SQL
"list_files_only" = "true"
```

Please note that you do not need to specify `data_format` when `list_files_only` is set to `true`.

For more information, see [Return](#return).

#### list_recursively

StarRocks further supports `list_recursively` to list the files and directories recursively. `list_recursively` only takes effect when `list_files_only` is set to `true`. The default value is `false`.

```SQL
"list_files_only" = "true",
"list_recursively" = "true"
```

When both `list_files_only` and `list_recursively` are set to `true`, StarRocks will do the follows:

- If the specified `path` is a file (whether it is specified specifically or represented by wildcards), StarRocks will show the information of the file.
- If the specified `path` is a directory (whether it is specified specifically or represented by wildcards, and whether or not it is suffixed by `/`), StarRocks will show all the files and sub-directories under this directory.

For more information, see [Return](#return).

### Return

#### SELECT FROM FILES()

When used with SELECT, FILES() returns the data in the file as a table.

- When querying CSV files, you can use `$1`, `$2` ... to represent each column in the SELECT statement, or specify `*` to obtain data from all columns.

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE $1 > 5;
  +------+---------+------------+
  | $1   | $2      | $3         |
  +------+---------+------------+
  |    6 | 0.34413 | 2017-11-25 |
  |    7 | 0.40055 | 2017-11-26 |
  |    8 | 0.42437 | 2017-11-27 |
  |    9 | 0.67935 | 2017-11-27 |
  |   10 | 0.22783 | 2017-11-29 |
  +------+---------+------------+
  5 rows in set (0.30 sec)

  SELECT $1, $2 FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------+
  | $1   | $2      |
  +------+---------+
  |    1 | 0.71173 |
  |    2 | 0.16145 |
  |    3 | 0.80524 |
  |    4 | 0.91852 |
  |    5 | 0.37766 |
  |    6 | 0.34413 |
  |    7 | 0.40055 |
  |    8 | 0.42437 |
  |    9 | 0.67935 |
  |   10 | 0.22783 |
  +------+---------+
  10 rows in set (0.38 sec)
  ```

- When querying Parquet or ORC files, you can directly specify the name of the desired columns in the SELECT statement, or specify `*` to obtain data from all columns.

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE c1 IN (101,105);
  +------+------+---------------------+
  | c1   | c2   | c3                  |
  +------+------+---------------------+
  |  101 |    9 | 2018-05-15T18:30:00 |
  |  105 |    6 | 2018-05-15T18:30:00 |
  +------+------+---------------------+
  2 rows in set (0.29 sec)

  SELECT c1, c3 FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------------------+
  | c1   | c3                  |
  +------+---------------------+
  |  101 | 2018-05-15T18:30:00 |
  |  102 | 2018-05-15T18:30:00 |
  |  103 | 2018-05-15T18:30:00 |
  |  104 | 2018-05-15T18:30:00 |
  |  105 | 2018-05-15T18:30:00 |
  |  106 | 2018-05-15T18:30:00 |
  |  107 | 2018-05-15T18:30:00 |
  |  108 | 2018-05-15T18:30:00 |
  |  109 | 2018-05-15T18:30:00 |
  |  110 | 2018-05-15T18:30:00 |
  +------+---------------------+
  10 rows in set (0.55 sec)
  ```

- When you query files with `list_files_only` set to `true`, the system will return `PATH`, `SIZE`, `IS_DIR` (whether the given path is a directory), and `MODIFICATION_TIME`.

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://bucket/*.parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "list_files_only" = "true"
  );
  +-----------------------+------+--------+---------------------+
  | PATH                  | SIZE | IS_DIR | MODIFICATION_TIME   |
  +-----------------------+------+--------+---------------------+
  | s3://bucket/1.parquet | 5221 |      0 | 2024-08-15 20:47:02 |
  | s3://bucket/2.parquet | 5222 |      0 | 2024-08-15 20:54:57 |
  | s3://bucket/3.parquet | 5223 |      0 | 2024-08-20 15:21:00 |
  | s3://bucket/4.parquet | 5224 |      0 | 2024-08-15 11:32:14 |
  +-----------------------+------+--------+---------------------+
  4 rows in set (0.03 sec)
  ```

- When you query files with `list_files_only` and `list_recursively` set to `true`, the system will list the files and directories recursively.

  Suppose the path `s3://bucket/list/` contains the following files and sub-directories:

  ```Plain
  s3://bucket/list/
  ├── basic1.csv
  ├── basic2.csv
  ├── orc0
  │   └── orc1
  │       └── basic_type.orc
  ├── orc1
  │   └── basic_type.orc
  └── parquet
      └── basic_type.parquet
  ```

  List the files and directories recursively:

  ```Plain
  SELECT * FROM FILES(
      "path"="s3://bucket/list/",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "list_files_only" = "true", 
      "list_recursively" = "true"
  );
  +---------------------------------------------+------+--------+---------------------+
  | PATH                                        | SIZE | IS_DIR | MODIFICATION_TIME   |
  +---------------------------------------------+------+--------+---------------------+
  | s3://bucket/list                            |    0 |      1 | 2024-12-24 22:15:59 |
  | s3://bucket/list/basic1.csv                 |   52 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/basic2.csv                 |   34 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0                       |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0/orc1                  |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0/orc1/basic_type.orc   | 1027 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc1                       |    0 |      1 | 2024-12-24 22:16:00 |
  | s3://bucket/list/orc1/basic_type.orc        | 1027 |      0 | 2024-12-24 22:16:00 |
  | s3://bucket/list/parquet                    |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/parquet/basic_type.parquet | 2281 |      0 | 2024-12-24 11:35:53 |
  +---------------------------------------------+------+--------+---------------------+
  10 rows in set (0.04 sec)
  ```

  Lists files and directories matching `orc*` in this path in a non-recursive way:

  ```Plain
  SELECT * FROM FILES(
      "path"="s3://bucket/list/orc*", 
      "list_files_only" = "true", 
      "list_recursively" = "false"
  );
  +--------------------------------------+------+--------+---------------------+
  | PATH                                 | SIZE | IS_DIR | MODIFICATION_TIME   |
  +--------------------------------------+------+--------+---------------------+
  | s3://bucket/list/orc0/orc1           |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc1/basic_type.orc | 1027 |      0 | 2024-12-24 22:16:00 |
  +--------------------------------------+------+--------+---------------------+
  2 rows in set (0.03 sec)
  ```


#### DESC FILES()

When used with DESC, FILES() returns the schema of the file.

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

When you viewing files with `list_files_only` set to `true`, the system will return the `Type` and `Null` properties of `PATH`, `SIZE`, `IS_DIR` (whether the given path is a directory), and `MODIFICATION_TIME`.

```Plain
DESC FILES(
    "path" = "s3://bucket/*.parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "list_files_only" = "true"
);
+-------------------+------------------+------+
| Field             | Type             | Null |
+-------------------+------------------+------+
| PATH              | varchar(1048576) | YES  |
| SIZE              | bigint           | YES  |
| IS_DIR            | boolean          | YES  |
| MODIFICATION_TIME | datetime         | YES  |
+-------------------+------------------+------+
4 rows in set (0.00 sec)
```

## FILES() for unloading

From v3.2.0 onwards, FILES() supports writing data into files in remote storage. You can use INSERT INTO FILES() to unload data from StarRocks to remote storage.

### Syntax

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
```

### Parameters

All parameters are in the `"key" = "value"` pairs.

#### data_location

See [FILES() for loading - Parameters - data_location](#data_location).

#### data_format

See [FILES() for loading - Parameters - data_format](#data_format).

#### StorageCredentialParams

See [FILES() for loading - Parameters - StorageCredentialParams](#storagecredentialparams).

#### unload_data_param

```sql
unload_data_param ::=
    "compression" = { "uncompressed" | "gzip" | "snappy" | "zstd | "lz4" },
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **Key**          | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| compression      | Yes          | The compression method to use when unloading data. Valid values:<ul><li>`uncompressed`: No compression algorithm is used.</li><li>`gzip`: Use the gzip compression algorithm.</li><li>`snappy`: Use the SNAPPY compression algorithm.</li><li>`zstd`: Use the Zstd compression algorithm.</li><li>`lz4`: Use the LZ4 compression algorithm.</li></ul>**NOTE**<br />Unloading into CSV files does not support data compression. You must set this item as `uncompressed`.                  |
| partition_by     | No           | The list of columns that are used to partition data files into different storage paths. Multiple columns are separated by commas (,). FILES() extracts the key/value information of the specified columns and stores the data files under the storage paths featured with the extracted key/value pair. For further instructions, see Example 7. |
| single           | No           | Whether to unload the data into a single file. Valid values:<ul><li>`true`: The data is stored in a single data file.</li><li>`false` (Default): The data is stored in multiple files if the amount of data unloaded exceeds 512 MB.</li></ul>                  |
| target_max_file_size | No           | The best-effort maximum size of each file in the batch to be unloaded. Unit: Bytes. Default value: 1073741824 (1 GB). When the size of data to be unloaded exceeds this value, the data will be divided into multiple files, and the size of each file will not significantly exceed this value. Introduced in v3.2.7. |

## Examples

#### Example 1: Query the data from a file

Query the data from the Parquet file **parquet/par-dup.parquet** within the AWS S3 bucket `inserttest`:

```Plain
SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
     "aws.s3.region" = "us-west-2"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

Query the data from the Parquet files in NFS(NAS):

```SQL
SELECT * FROM FILES(
  'path' = 'file:///home/ubuntu/parquetfile/*.parquet', 
  'format' = 'parquet'
);
```

#### Example 2: Insert the data rows from a file

Insert the data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table `insert_wiki_edit`:

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```

Insert the data rows from the CSV files in NFS(NAS) into the table `insert_wiki_edit`:

```SQL
INSERT INTO insert_wiki_edit
  SELECT * FROM FILES(
    'path' = 'file:///home/ubuntu/csvfile/*.csv', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimiter' = '\n'
  );
```

#### Example 3: CTAS with data rows from a file

Create a table named `ctas_wiki_edit` and insert the data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table:

```Plain
CREATE TABLE ctas_wiki_edit AS
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```

#### Example 4: Query the data from a file and extract the key/value information in its path

Query the data from the Parquet file **/geo/country=US/city=LA/file1.parquet** (which only contains two columns -`id` and `user`), and extract the key/value information in its path as columns returned.

```Plain
SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/geo/country=US/city=LA/file1.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "columns_from_path" = "country, city"
);
+------+---------+---------+------+
| id   | user    | country | city |
+------+---------+---------+------+
|    1 | richard | US      | LA   |
|    2 | amber   | US      | LA   |
+------+---------+---------+------+
2 rows in set (3.84 sec)
```

#### Example 5: Automatic schema detection and Unionization

The following example is based on two Parquet files in the S3 bucket:

- File 1 contains three columns - INT column `c1`, FLOAT column `c2`, and DATE column `c3`.

```Plain
c1,c2,c3
1,0.71173,2017-11-20
2,0.16145,2017-11-21
3,0.80524,2017-11-22
4,0.91852,2017-11-23
5,0.37766,2017-11-24
6,0.34413,2017-11-25
7,0.40055,2017-11-26
8,0.42437,2017-11-27
9,0.67935,2017-11-27
10,0.22783,2017-11-29
```

- File 2 contains three columns - INT column `c1`, INT column `c2`, and DATETIME column `c3`.

```Plain
c1,c2,c3
101,9,2018-05-15T18:30:00
102,3,2018-05-15T18:30:00
103,2,2018-05-15T18:30:00
104,3,2018-05-15T18:30:00
105,6,2018-05-15T18:30:00
106,1,2018-05-15T18:30:00
107,8,2018-05-15T18:30:00
108,5,2018-05-15T18:30:00
109,6,2018-05-15T18:30:00
110,8,2018-05-15T18:30:00
```

Use a CTAS statement to create a table named `test_ctas_parquet` and insert the data rows from the two Parquet files into the table:

```SQL
CREATE TABLE test_ctas_parquet AS
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/*",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```

View the table schema of `test_ctas_parquet`:

```SQL
SHOW CREATE TABLE test_ctas_parquet\G
```

```Plain
*************************** 1. row ***************************
       Table: test_ctas_parquet
Create Table: CREATE TABLE `test_ctas_parquet` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
```

The result shows that the `c2` column, which contains both FLOAT and INT data, is merged as a DECIMAL column, and `c3`, which contains both DATE and DATETIME data, is merged as a VARCHAR column.

The above result stays the same when the Parquet files are changed to CSV files that contain the same data:

```Plain
CREATE TABLE test_ctas_csv AS
  SELECT * FROM FILES(
    "path" = "s3://inserttest/csv/*",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
Query OK, 0 rows affected (30.90 sec)

SHOW CREATE TABLE test_ctas_csv\G
*************************** 1. row ***************************
       Table: test_ctas_csv
Create Table: CREATE TABLE `test_ctas_csv` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
1 row in set (0.27 sec)
```

- Unionize the schema of Parquet files and allow the system to assign NULL values to non-existent columns by setting `fill_mismatch_column_with` to `null`:

```SQL
SELECT * FROM FILES(
  "path" = "s3://inserttest/basic_type.parquet,s3://inserttest/basic_type_k2k5k7.parquet",
  "format" = "parquet",
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "us-west-2",
  "fill_mismatch_column_with" = "null"
);
+------+------+------+-------+------------+---------------------+------+------+
| k1   | k2   | k3   | k4    | k5         | k6                  | k7   | k8   |
+------+------+------+-------+------------+---------------------+------+------+
| NULL |   21 | NULL |  NULL | 2024-10-03 | NULL                | c    | NULL |
|    0 |    1 |    2 |  3.20 | 2024-10-01 | 2024-10-01 12:12:12 | a    |  4.3 |
|    1 |   11 |   12 | 13.20 | 2024-10-02 | 2024-10-02 13:13:13 | b    | 14.3 |
+------+------+------+-------+------------+---------------------+------+------+
3 rows in set (0.03 sec)
```

#### Example 6: View the schema of a file

View the schema of the Parquet file `lineorder` stored in AWS S3 using DESC.

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

#### Example 7: Unload data

Unload all data rows in `sales_records` as multiple Parquet files under the path **/unload/partitioned/** in the HDFS cluster. These files are stored in different subpaths distinguished by the values in the column `sales_time`.

```SQL
INSERT INTO FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "partition_by" = "sales_time"
)
SELECT * FROM sales_records;
```

Unload the query results into CSV and Parquet files in NFS(NAS):

```SQL
-- CSV
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/csvfile/', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimitor' = '\n'
)
SELECT * FROM sales_records;

-- Parquet
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/parquetfile/',
    'format' = 'parquet'
)
SELECT * FROM sales_records;
```

#### Example 8: Avro files

Load an Avro file:

```SQL
INSERT INTO avro_tbl
  SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xx.x:yyyy/avro/primitive.avro", 
    "format" = "avro"
);
```

Query the data from an Avro file:

```SQL
SELECT * FROM FILES("path" = "hdfs://xxx.xx.xx.x:yyyy/avro/complex.avro", "format" = "avro")\G
*************************** 1. row ***************************
record_field: {"id":1,"name":"avro"}
  enum_field: HEARTS
 array_field: ["one","two","three"]
   map_field: {"a":1,"b":2}
 union_field: 100
 fixed_field: 0x61626162616261626162616261626162
1 row in set (0.05 sec)
```

View the schema of an Avro file:

```SQL
DESC FILES("path" = "hdfs://xxx.xx.xx.x:yyyy/avro/logical.avro", "format" = "avro");
+------------------------+------------------+------+
| Field                  | Type             | Null |
+------------------------+------------------+------+
| decimal_bytes          | decimal(10,2)    | YES  |
| decimal_fixed          | decimal(10,2)    | YES  |
| uuid_string            | varchar(1048576) | YES  |
| date                   | date             | YES  |
| time_millis            | int              | YES  |
| time_micros            | bigint           | YES  |
| timestamp_millis       | datetime         | YES  |
| timestamp_micros       | datetime         | YES  |
| local_timestamp_millis | bigint           | YES  |
| local_timestamp_micros | bigint           | YES  |
| duration               | varbinary(12)    | YES  |
+------------------------+------------------+------+
```

#### Example 9: Access Azure Blob Storage using Managed Identity and Service Principal

```SQL
-- Managed Identity
SELECT * FROM FILES(
    "path" = "wasbs://storage-container@storage-account.blob.core.windows.net/ssb_1g/customer/*",
    "format" = "parquet",
    "azure.blob.oauth2_use_managed_identity" = "true",
    "azure.blob.oauth2_client_id" = "1d6bfdec-dd34-4260-b8fd-aaaaaaaaaaaa"
);
-- Service Principal
SELECT * FROM FILES(
    "path" = "wasbs://storage-container@storage-account.blob.core.windows.net/ssb_1g/customer/*",
    "format" = "parquet",
    "azure.blob.oauth2_client_id" = "1d6bfdec-dd34-4260-b8fd-bbbbbbbbbbbb",
    "azure.blob.oauth2_client_secret" = "C2M8Q~ZXXXXXX_5XsbDCeL2dqP7hIR60xxxxxxxx",
    "azure.blob.oauth2_tenant_id" = "540e19cc-386b-4a44-a7b8-cccccccccccc"
);
```

#### Example 10: CSV file

Query the data from a CSV file:

```SQL
SELECT * FROM FILES(                                                                                                                                                     "path" = "s3://test-bucket/file1.csv",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\r\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
+------+---------+--------------+
| $1   | $2      | $3           |
+------+---------+--------------+
|    1 | 0.71173 | 2017-11-20   |
|    2 | 0.16145 | 2017-11-21   |
|    3 | 0.80524 | 2017-11-22   |
|    4 | 0.91852 | 2017-11-23   |
|    5 | 0.37766 | 2017-11-24   |
|    6 | 0.34413 | 2017-11-25   |
|    7 | 0.40055 | 2017-11-26   |
|    8 | 0.42437 | 2017-11-27   |
|    9 | 0.67935 | 2017-11-27   |
|   10 | 0.22783 | 2017-11-29   |
+------+---------+--------------+
10 rows in set (0.33 sec)
```

Load a CSV file:

```SQL
INSERT INTO csv_tbl
  SELECT * FROM FILES(
    "path" = "s3://test-bucket/file1.csv",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\r\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```
