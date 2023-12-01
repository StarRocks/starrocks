---
displayed_sidebar: "English"
---

# Deploy StarRocks using GCS 

import SharedDataIntro from '../../assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## Architecture

![Shared-data Architecture](../../assets/share_data_arch.png)

## Deploy a shared-data StarRocks cluster

The deployment of a shared-data StarRocks cluster is similar to that of a shared-nothing StarRocks cluster. The only difference is that you need to deploy CNs instead of BEs in a shared-data cluster. This section only lists the extra FE and CN configuration items you need to add in the configuration files of FE and CN **fe.conf** and **cn.conf** when you deploy a shared-data StarRocks cluster. For detailed instructions on deploying a StarRocks cluster, see [Deploy StarRocks](../../deployment/deploy_manually.md).

> **Note**
>
> Do not start the cluster until after it is configured for shared-storage in the next section of this document.

## Configure FE nodes for shared-data StarRocks

Before starting the cluster configure the FEs and CNs. An example configuration is provided below, and then the details for each parameter are provided.

### Example FE configuration for GCS

The example shared-data additions for your `fe.conf` can be added to the `fe.conf` file on each
of your FE nodes. Because GCS storage is accessed using the
[Cloud Storage XML API](https://cloud.google.com/storage/docs/xml-api/overview), the parameters
use the prefix `aws_s3`.

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = S3

  # For example, testbucket/subpath
  aws_s3_path = <s3_path>

  # For example: us-east1
  aws_s3_region = <region>

  # For example: https://storage.googleapis.com
  aws_s3_endpoint = <endpoint_url>

  aws_s3_access_key = <HMAC access_key>
  aws_s3_secret_key = <HMAC secret_key>
  ```

### All FE parameters related to shared-storage with GCS

#### run_mode

The running mode of the StarRocks cluster. Valid values: 

- `shared_data`
- `shared_nothing` (Default).

> **Note**
>
> You cannot adopt the `shared_data` and `shared_nothing` modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.
>
> Do not change `run_mode` after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.

#### cloud_native_meta_port

The cloud-native meta service RPC port.

- Default: `6090`

#### enable_load_volume_from_conf

Whether to allow StarRocks to create the default storage volume by using the object storage-related properties specified in the FE configuration file. Valid values:

- `true` (Default) If you specify this item as `true` when creating a new shared-data cluster, StarRocks creates the built-in storage volume `builtin_storage_volume` using the object storage-related properties in the FE configuration file, and sets it as the default storage volume. However, if you have not specified the object storage-related properties, StarRocks fails to start.
- `false` If you specify this item as `false` when creating a new shared-data cluster, StarRocks starts directly without creating the built-in storage volume. You must manually create a storage volume and set it as the default storage volume before creating any object in StarRocks. For more information, see [Create the default storage volume](#create-default-storage-volume).

Supported from v3.1.0.

> **CAUTION**
>
> We strongly recommend you leave this item as `true` while you are upgrading an existing shared-data cluster from v3.0. If you specify this item as `false`, the databases and tables you created before the upgrade become read-only, and you cannot load data into them.

#### cloud_native_storage_type

The type of object storage you use. In shared-data mode, StarRocks supports storing data in Azure Blob (supported from v3.1.1 onwards), and object storages that are compatible with the S3 protocol (such as AWS S3, Google GCS, and MinIO). Valid value:

- `S3` (Default)
- `AZBLOB`.

#### aws_s3_path

The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.

#### aws_s3_endpoint

The endpoint used to access your S3 bucket, for example, `https://storage.googleapis.com/`

#### aws_s3_region

The region in which your S3 bucket resides, for example, `us-west-2`.

#### aws_s3_use_instance_profile

Whether to use Instance Profile and Assumed Role as credential methods for accessing GCS. Valid values:

- `true`
- `false` (Default).

If you use IAM user-based credential (Access Key and Secret Key) to access GCS, you must specify this item as `false`, and specify `aws_s3_access_key` and `aws_s3_secret_key`.

If you use Instance Profile to access GCS, you must specify this item as `true`.

If you use Assumed Role to access GCS, you must specify this item as `true`, and specify `aws_s3_iam_role_arn`.

And if you use an external AWS account,  you must also specify `aws_s3_external_id`.

#### aws_s3_access_key

The HMAC access Key ID used to access your GCS bucket.

#### aws_s3_secret_key

The HMAC Secret Access Key used to access your GCS bucket.

#### aws_s3_iam_role_arn

The ARN of the IAM role that has privileges on your GCS bucket in which your data files are stored.

#### aws_s3_external_id

The external ID of the AWS account that is used for cross-account access to your GCS bucket.

> **Note**
>
> Only credential-related configuration items can be modified after your shared-data StarRocks cluster is created. If you changed the original storage path-related configuration items, the databases and tables you created before the change become read-only, and you cannot load data into them.

If you want to create the default storage volume manually after the cluster is created, you only need to add the following configuration items:

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
enable_load_volume_from_conf = false
```

## Configure CN nodes for shared-data StarRocks
<SharedDataCNconf />

## Use your shared-data StarRocks cluster

<SharedDataUseIntro />

The following example creates a storage volume `def_volume` for a GCS bucket `defaultbucket` with an HMAC Access Key and Secret Key, enables the storage volume, and sets it as the default storage volume:

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "us-east1",
    "aws.s3.endpoint" = "https://storage.googleapis.com",
    "aws.s3.access_key" = "<HMAC access key>",
    "aws.s3.secret_key" = "<HMAC secret key>"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

<SharedDataUse />
