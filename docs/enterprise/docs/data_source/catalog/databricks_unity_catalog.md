# Databricks Unity Catalog

StarRocks supports accessing Unity Catalog by creating a Delta Lake catalog.

Below, we will introduce how to create a [personal access token](https://docs.databricks.com/aws/en/dev-tools/auth/pat) in Databricks and configure the corresponding storage parameters to query Delta Lake tables in Unity Catalog.

## Create Databricks personal access tokens
To create a Databricks personal access token for your Databricks workspace user, do the following:

1. In your Databricks workspace, click your Databricks username in the top bar, and then select **Settings** from the drop down.

2. Click **Developer**.

3. Next to **Access tokens**, click **Manage**.

4. Click **Generate new token**.

5. Enter a comment that helps you to identify this token in the future.

6. Set the token’s lifetime in days.

    If you leave the **Lifetime (days)** box empty, the token lifetime is set to the maximum lifetime for your workspace. By default, the maximum token lifetime for a workspace is 730 days.
    
7. Click **Generate**.

8. Copy the displayed token to a secure location, and then click **Done**.

> **NOTE**
> 
> If you are not able to create or use tokens in your workspace, this might be because your workspace administrator has disabled tokens or has not given you permission to create or use tokens. See your workspace administrator or the following topics:
> 
>  1. [Enable or disable personal access token authentication for the workspace](https://docs.databricks.com/aws/en/admin/access-control/tokens#enable-tokens)
> 
>  2. [Personal access token permissions](https://docs.databricks.com/aws/en/security/auth/api-access-permissions#personal-access-token-permissions)

If you want to access the Unity Catalog through a **service principal** instead of a workspace user, you need to create personal access tokens for the service principal.
You can refer to this [doc](https://docs.databricks.com/aws/en/dev-tools/auth/pat#databricks-personal-access-tokens-for-service-principals) for the creation process


## Create Delta Lake Catalog

### MetastoreParams
If you choose Databricks Unity Catalog as the metastore of your data source, configure `MetastoreParams` as follows:

```SQL
"hive.metastore.type" = "unity",
"databricks.host"= "https://<host>.cloud.databricks.com",
"databricks.token" = "<token>",
"databricks.catalog.name" = "<catalog_name>",
```

| Parameter               | Required | Description                                                                                                                                                                                                                      |
| :---------------------- | :------- |:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hive.metastore.type     | Yes      | The type of metastore that you use for your Delta Lake cluster. Set the value to `unity`.                                                                                                                                        |
| databricks.host         | Yes      | The host of your Databricks cluster, for example, `https://abcd1234.cloud.databricks.com`. you can refer to [workspace URL](https://docs.databricks.com/aws/en/workspace/workspace-details#workspace-url) to get this parameter. |
| databricks.token        | Yes      | The token used to access your Unity Catalog metastore, paste the **personal access token** obtained above here.                                                                                                                  |
| databricks.catalog.name | Yes      | The name of your Unity Catalog.                                                                                                                                                                                                  |


### StorageCredentialParams
> **NOTE**
>
> By setting **MetastoreParams**, StarRocks can access the Unity Catalog to obtain the database/table names and **table locations**, but it cannot read the data of the table.
> This is because both the data and metadata files (delta_log) of the Delta Lake table are stored in the storage system.
> StarRocks needs to configure the corresponding storage system authentication parameters to ensure that StarRocks has the **permission** to read the files in Delta Lake table location.

The Delta Lake catalog supports **AWS S3**, **Microsoft Azure Storage**, and **Google GCS** as storage.
You can refer [here](./deltalake_catalog.md#storagecredentialparams) to configure storage parameters

### Examples
The following examples create a Delta Lake catalog named databricks_unity_catalog, to query data in databricks unity catalog.

If you choose AWS **IAM user-based credential**
```SQL
create external catalog databricks_unity_catalog properties (
    "type"="deltalake",
    "hive.metastore.type" = "unity",
    "databricks.host"= "https://xxxxxx.cloud.databricks.com",
    "databricks.token" = "<personal_access_token>",
    "databricks.catalog.name" = "<unity_catalog_name>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_access_key>",
    "aws.s3.region"  =  "<aws_s3_region>"
);
```

If you choose AWS **instance profile-based credential**
```SQL
create external catalog databricks_unity_catalog properties (
    "type"="deltalake",
    "hive.metastore.type" = "unity",
    "databricks.host"= "https://xxxxxx.cloud.databricks.com",
    "databricks.token" = "<personal_access_token>",
    "databricks.catalog.name" = "<unity_catalog_name>",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region"  =  "<aws_s3_region>"
);
```

If you choose AWS **assumed role-based credential**
```SQL
create external catalog databricks_unity_catalog properties (
    "type"="deltalake",
    "hive.metastore.type" = "unity",
    "databricks.host"= "https://xxxxxx.cloud.databricks.com",
    "databricks.token" = "<personal_access_token>",
    "databricks.catalog.name" = "<unity_catalog_name>",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "<iam_role_arn>",
    "aws.s3.region" = "<aws_s3_region>"
);
```

## Limitations
The **table types** supported for querying by StarRocks are as follows:

| Table type                              | Status      |
|:----------------------------------------|-------------|
| Unity Catalog managed table             | Support     |
| Unity Catalog external delta lake table | Support     |
| Streaming table                         | Not Support |
| Materialized view                       | Not Support |
| View                                    | Not Support |
