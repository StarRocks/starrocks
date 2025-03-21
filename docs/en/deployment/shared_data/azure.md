---
displayed_sidebar: docs
---

# Use Azure Storage for shared-data

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## Architecture

![Shared-data Architecture](../../_assets/share_data_arch.png)

## Deploy a shared-data StarRocks cluster

The deployment of a shared-data StarRocks cluster is similar to that of a shared-nothing StarRocks cluster. The only difference is that you need to deploy CNs instead of BEs in a shared-data cluster. This section only lists the extra FE and CN configuration items you need to add in the configuration files of FE and CN **fe.conf** and **cn.conf** when you deploy a shared-data StarRocks cluster. For detailed instructions on deploying a StarRocks cluster, see [Deploy StarRocks](../../deployment/deploy_manually.md).

> **NOTE**
>
> Do not start the cluster until after it is configured for shared-storage in the next section of this document.

## Configure FE nodes for shared-data StarRocks

Before starting the cluster configure the FEs and CNs. An example configuration is provided below, and then the details for each parameter are provided.

### Example FE configuration for Azure Storage

The example shared-data additions for your `fe.conf` can be added to the `fe.conf` file on each of your FE nodes.

**Azure Blob Storage**

- If you use the shared key to access Azure Blob Storage, add the following configuration items:

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = AZBLOB

  # For example, testcontainer/subpath
  azure_blob_path = <blob_path>

  # For example, https://test.blob.core.windows.net
  azure_blob_endpoint = <endpoint_url>

  azure_blob_shared_key = <shared_key>
  ```

- If you use shared access signatures (SAS) to access Azure Blob Storage, add the following configuration items:

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = AZBLOB

  # For example, testcontainer/subpath
  azure_blob_path = <blob_path>

  # For example, https://test.blob.core.windows.net
  azure_blob_endpoint = <endpoint_url>

  azure_blob_sas_token = <sas_token>
  ```

> **CAUTION**
>
> The hierarchical namespace must be disabled when you create the Azure Blob Storage Account.

**Azure Data Lake Storage Gen2**

- If you use the shared key to access Azure Data Lake Storage Gen2, add the following configuration items:

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = ADLS2

  # For example, testfilesystem/starrocks
  azure_adls2_path = <file_system_name>/<dir_name>

  # For example, https://test.dfs.core.windows.net
  azure_adls2_endpoint = <endpoint_url>

  azure_adls2_shared_key = <shared_key>
  ```

- If you use shared access signatures (SAS) to access Azure Data Lake Storage Gen2, add the following configuration items:

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = ADLS2

  # For example, testfilesystem/starrocks
  azure_adls2_path = <file_system_name>/<dir_name>

  # For example, https://test.dfs.core.windows.net
  azure_adls2_endpoint = <endpoint_url>

  azure_adls2_sas_token = <sas_token>
  ```

> **NOTE**
>
> Azure Data Lake Storage Gen1 is not supported.

### All FE parameters related to shared-storage with Azure Storage

#### run_mode

The running mode of the StarRocks cluster. Valid values:

- `shared_data`
- `shared_nothing` (Default)

> **NOTE**
>
> - You cannot adopt the `shared_data` and `shared_nothing` modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.
> - Do not change `run_mode` after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.

#### cloud_native_meta_port

The cloud-native meta service RPC port.

- Default: `6090`

#### enable_load_volume_from_conf

Whether to allow StarRocks to create the default storage volume by using the object storage-related properties specified in the FE configuration file. Valid values:

- `true` If you specify this item as `true` when creating a new shared-data cluster, StarRocks creates the built-in storage volume `builtin_storage_volume` using the object storage-related properties in the FE configuration file, and sets it as the default storage volume. However, if you have not specified the object storage-related properties, StarRocks fails to start.
- `false` (Default) If you specify this item as `false` when creating a new shared-data cluster, StarRocks starts directly without creating the built-in storage volume. You must manually create a storage volume and set it as the default storage volume before creating any object in StarRocks. For more information, see [Create the default storage volume](#use-your-shared-data-starrocks-cluster).

Supported from v3.1.0.

> **CAUTION**
>
> We strongly recommend you leave this item as `true` while you are upgrading an existing shared-data cluster from v3.0. If you specify this item as `false`, the databases and tables you created before the upgrade become read-only, and you cannot load data into them.

#### cloud_native_storage_type

The type of object storage you use. In shared-data mode, StarRocks supports storing data in Azure Blob (supported from v3.1.1 onwards), and object storages that are compatible with the S3 protocol (such as AWS S3, Google GCP, and MinIO). Valid value:

- `S3` (Default)
- `AZBLOB`
- `HDFS`

> **NOTE**
>
> - If you specify this parameter as `S3`, you must add the parameters prefixed by `aws_s3`.
> - If you specify this parameter as `AZBLOB`, you must add the parameters prefixed by `azure_blob`.
> - If you specify this parameter as `HDFS`, you must add the parameter `cloud_native_hdfs_url`.

#### azure_blob_path

The Azure Blob Storage path used to store data. It consists of the name of the container within your storage account and the sub-path (if any) under the container, for example, `testcontainer/subpath`.

#### azure_blob_endpoint

The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`.

#### azure_blob_shared_key

The Shared Key used to authorize requests for your Azure Blob Storage.

#### azure_blob_sas_token

The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage.

#### azure_adls2_path

The Azure Data Lake Storage Gen2 path used to store data. It consists of the file system name and the directory name, for example, `testfilesystem/starrocks`.

#### azure_adls2_endpoint

The endpoint of your Azure Data Lake Storage Gen2 Account, for example, `https://test.dfs.core.windows.net`.

#### azure_adls2_shared_key

The Shared Key used to authorize requests for your Azure Data Lake Storage Gen2.

#### azure_adls2_sas_token

The shared access signatures (SAS) used to authorize requests for your Azure Data Lake Storage Gen2.

> **NOTE**
>
> Only credential-related configuration items can be modified after your shared-data StarRocks cluster is created. If you changed the original storage path-related configuration items, the databases and tables you created before the change become read-only, and you cannot load data into them.

If you want to create the default storage volume manually after the cluster is created, you only need to add the following configuration items:

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
```

## Configure CN nodes for shared-data StarRocks

<SharedDataCNconf />

## Use your shared-data StarRocks cluster

<SharedDataUseIntro />

The following example creates a storage volume `def_volume` for an Azure Blob Storage bucket `defaultbucket` with shared key access, enables the storage volume, and sets it as the default storage volume:

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = AZBLOB
LOCATIONS = ("azblob://defaultbucket/test/")
PROPERTIES
(
    "enabled" = "true",
    "azure.blob.endpoint" = "<endpoint_url>",
    "azure.blob.shared_key" = "<shared_key>"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

The following example creates a storage volume `adls2` for an Azure Data Lake Storage Gen2 file system `testfilesystem` with SAS token, and disables the storage volume:

```SQL
CREATE STORAGE VOLUME adls2
    TYPE = ADLS2
    LOCATIONS = ("adls2://testfilesystem/starrocks")
    PROPERTIES (
        "enabled" = "false",
        "azure.adls2.endpoint" = "<endpoint_url>",
        "azure.adls2.sas_token" = "<sas_token>"
    );
```

<SharedDataUse />
