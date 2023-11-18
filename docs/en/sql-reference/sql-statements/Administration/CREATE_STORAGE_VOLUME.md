---
displayed_sidebar: "English"
---

# CREATE STORAGE VOLUME

## Description

Creates a storage volume for a remote storage system. This feature is supported from v3.1.

A storage volume consists of the properties and credential information of the remote data storage. You can reference a storage volume when you create databases and cloud-native tables in a [shared-data StarRocks cluster](../../../deployment/shared_data/s3.md).

> **CAUTION**
>
> Only users with the CREATE STORAGE VOLUME privilege on the SYSTEM level can perform this operation.

## Syntax

```SQL
CREATE STORAGE VOLUME [IF NOT EXISTS] <storage_volume_name>
TYPE = { S3 | AZBLOB }
LOCATIONS = ('<remote_storage_path>')
[ COMMENT '<comment_string>' ]
PROPERTIES
("key" = "value",...)
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | The name of the storage volume. Please note that you cannot create a storage volume named `builtin_storage_volume` because it is used to create the builtin storage volume. |
| TYPE                | The type of the remote storage system. Valid values: `S3` and `AZBLOB`. `S3` indicates AWS S3 or S3-compatible storage systems. `AZBLOB` indicates Azure Blob Storage (supported from v3.1.1 onwards). |
| LOCATIONS           | The storage locations. The format is as follows:<ul><li>For AWS S3 or S3 protocol-compatible storage systems: `s3://<s3_path>`. `<s3_path>` must be an absolute path, for example, `s3://testbucket/subpath`.</li><li>For Azure Blob Storage: `azblob://<azblob_path>`. `<azblob_path>` must be an absolute path, for example, `azblob://testcontainer/subpath`.</li></ul> |
| COMMENT             | The comment on the storage volume.                           |
| PROPERTIES          | Parameters in the `"key" = "value"` pairs used to specify the properties and credential information to access the remote storage system. For detailed information, see [PROPERTIES](#properties). |

### PROPERTIES

- If you use AWS S3:

  - If you use the default authentication credential of AWS SDK to access S3, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "true"
    ```

  - If you use IAM user-based credential (Access Key and Secret Key) to access S3, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secrete_key>"
    ```

  - If you use Instance Profile to access S3, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "true"
    ```

  - If you use Assumed Role to access S3, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "<role_arn>"
    ```

  - If you use Assumed Role to access S3 from an external AWS account, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "<role_arn>",
    "aws.s3.external_id" = "<external_id>"
    ```

- If you use GCP Cloud Storage, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  
  -- For example: us-east-1
  "aws.s3.region" = "<region>",
  
  -- For example: https://storage.googleapis.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- If you use MinIO, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  
  -- For example: us-east-1
  "aws.s3.region" = "<region>",
  
  -- For example: http://172.26.xx.xxx:39000
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

  | **Property**                        | **Description**                                              |
  | ----------------------------------- | ------------------------------------------------------------ |
  | enabled                             | Whether to enable this storage volume. Default: `false`. Disabled storage volume cannot be referenced. |
  | aws.s3.region                       | The region in which your S3 bucket resides, for example, `us-west-2`. |
  | aws.s3.endpoint                     | The endpoint URL used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`. |
  | aws.s3.use_aws_sdk_default_behavior | Whether to use the default authentication credential of AWS SDK. Valid values: `true` and `false` (Default). |
  | aws.s3.use_instance_profile         | Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: `true` and `false` (Default).<ul><li>If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as `false`, and specify `aws.s3.access_key` and `aws.s3.secret_key`.</li><li>If you use Instance Profile to access S3, you must specify this item as `true`.</li><li>If you use Assumed Role to access S3, you must specify this item as `true`, and specify `aws.s3.iam_role_arn`.</li><li>And if you use an external AWS account, you must specify this item as `true`, and specify `aws.s3.iam_role_arn` and `aws.s3.external_id`.</li></ul> |
  | aws.s3.access_key                   | The Access Key ID used to access your S3 bucket.             |
  | aws.s3.secret_key                   | The Secret Access Key used to access your S3 bucket.         |
  | aws.s3.iam_role_arn                 | The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored. |
  | aws.s3.external_id                  | The external ID of the AWS account that is used for cross-account access to your S3 bucket. |

- If you use Azure Blob Storage (supported from v3.1.1 onwards):

  - If you use Shared Key to access Azure Blob Storage, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "azure.blob.endpoint" = "<endpoint_url>",
    "azure.blob.shared_key" = "<shared_key>"
    ```

  - If you use shared access signatures (SAS) to access Azure Blob Storage, set the following properties:

    ```SQL
    "enabled" = "{ true | false }",
    "azure.blob.endpoint" = "<endpoint_url>",
    "azure.blob.sas_token" = "<sas_token>"
    ```

  > **CAUTION**
  >
  > The hierarchical namespace must be disabled when you create the Azure Blob Storage Account.

  | **Property**          | **Description**                                              |
  | --------------------- | ------------------------------------------------------------ |
  | enabled               | Whether to enable this storage volume. Default: `false`. Disabled storage volume cannot be referenced. |
  | azure.blob.endpoint   | The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`. |
  | azure.blob.shared_key | The Shared Key used to authorize requests for your Azure Blob Storage. |
  | azure.blob.sas_token  | The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage. |

## Examples

Example 1: Create a storage volume `my_s3_volume` for the AWS S3 bucket `defaultbucket`, use the IAM user-based credential (Access Key and Secret Key) to access S3, and enable it.

```SQL
CREATE STORAGE VOLUME my_s3_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);
```

## Relevant SQL statements

- [ALTER STORAGE VOLUME](./ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./SHOW_STORAGE_VOLUMES.md)
