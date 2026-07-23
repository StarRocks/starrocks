---
displayed_sidebar: docs
description: "Use a Fluss catalog to query data in Apache Fluss tables."
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Fluss catalog

<Beta />

A Fluss catalog is an external catalog that enables you to query Apache Fluss tables without loading data into StarRocks.

## Usage notes

In the current version, Fluss catalogs support querying only Fluss tables that have DataLake enabled and use Apache Paimon as the lake storage.

## Create a Fluss catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "fluss",
    "bootstrap.servers" = "<fluss_bootstrap_server>",
    "fluss.option.<fluss_client_option>" = "<value>",
    "table.datalake.paimon.<paimon_option>" = "<value>"
);
```

### Parameters

#### catalog_name

The name of the Fluss catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the Fluss catalog. This parameter is optional.

#### PROPERTIES

| Parameter | Required | Description |
| --- | --- | --- |
| `type` | Yes | The data source type. Set the value to `fluss`. |
| `bootstrap.servers` | Yes | The address that StarRocks uses to connect to Fluss. Example: `fluss-host:9123`. |
| `fluss.option.<fluss_client_option>` | No | A Fluss client option. Remove the `fluss.option.` prefix to obtain the corresponding Fluss client configuration key. For example, `fluss.option.client.security.protocol` sets `client.security.protocol`. |
| `table.datalake.paimon.<paimon_option>` | No | A Paimon option used by StarRocks to access the lake storage of a Fluss table. |

### Example

The following example creates a Fluss catalog that uses Paimon filesystem metastore and S3 storage:

```SQL
CREATE EXTERNAL CATALOG fluss_catalog
PROPERTIES
(
    "type" = "fluss",
    "bootstrap.servers" = "fluss-host:9123",

    -- Optional. Configure these properties when Fluss client
    -- authentication is enabled.
    "fluss.option.client.security.protocol" = "SASL",
    "fluss.option.client.security.sasl.mechanism" = "PLAIN",
    "fluss.option.client.security.sasl.username" = "<fluss_user>",
    "fluss.option.client.security.sasl.password" = "<fluss_password>",

    -- Paimon lake storage.
    "table.datalake.paimon.metastore" = "filesystem",
    "table.datalake.paimon.warehouse" = "s3://bucket/path",
    "table.datalake.paimon.s3.endpoint" = "https://s3.<region>.amazonaws.com",
    "table.datalake.paimon.s3.access-key" = "<access_key>",
    "table.datalake.paimon.s3.secret-key" = "<secret_key>"

    -- To use Alibaba Cloud OSS, replace the S3 warehouse and properties
    -- above with the following properties:
    -- "table.datalake.paimon.warehouse" = "oss://<bucket>/<path>",
    -- "table.datalake.paimon.fs.oss.endpoint" = "oss-cn-hangzhou.aliyuncs.com",
    -- "table.datalake.paimon.fs.oss.accessKeyId" = "<access_key_id>",
    -- "table.datalake.paimon.fs.oss.accessKeySecret" = "<access_key_secret>"
);
```

## View Fluss catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog:

```SQL
SHOW CREATE CATALOG fluss_catalog;
```

## Drop a Fluss catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop an external catalog:

```SQL
DROP CATALOG fluss_catalog;
```

## View the schema of a Fluss table

You can use one of the following syntaxes to view the schema of a Fluss table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Query a Fluss table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the databases in Fluss:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) to switch to the Fluss catalog in the current session:

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   Then, use [USE](../../sql-reference/sql-statements/Database/USE.md) to specify the active database:

   ```SQL
   USE <db_name>;
   ```

   Or, directly specify the active database in the Fluss catalog:

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) to query a Fluss table:

   ```SQL
   -- Union read: historical data in the lake and real-time data in Fluss.
   SELECT * FROM <table_name>;

   -- Lake read: historical data in the lake only.
   SELECT * FROM <table_name>$lake;

   -- Real-time read: real-time data in Fluss only.
   SELECT * FROM <table_name>$rt;
   ```
