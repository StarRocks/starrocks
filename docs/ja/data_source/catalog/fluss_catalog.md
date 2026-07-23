---
displayed_sidebar: docs
description: "Fluss catalog を使用して Apache Fluss テーブルのデータをクエリします。"
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Fluss catalog

<Beta />

Fluss catalog は、データを StarRocks にロードせずに Apache Fluss テーブルをクエリできる External Catalog です。

## 使用上の注意

現在のバージョンでは、Fluss catalog は DataLake が有効で、lake storage として Apache Paimon を使用する Fluss テーブルのクエリのみをサポートします。

## Fluss catalog の作成

### 構文

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

### パラメータ

#### catalog_name

Fluss catalog の名前。命名規則は次のとおりです。

- 名前には英字、数字（0-9）、およびアンダースコア（_）を使用でき、英字で始める必要があります。
- 名前では大文字と小文字が区別され、長さは 1023 文字以下である必要があります。

#### comment

Fluss catalog の説明。このパラメータはオプションです。

#### PROPERTIES

| パラメータ | 必須 | 説明 |
| --- | --- | --- |
| `type` | はい | データソースの種類。値を `fluss` に設定します。 |
| `bootstrap.servers` | はい | StarRocks が Fluss への接続に使用するアドレス。例: `fluss-host:9123`。 |
| `fluss.option.<fluss_client_option>` | いいえ | Fluss クライアントオプション。`fluss.option.` プレフィックスを削除すると、対応する Fluss クライアント設定キーになります。たとえば、`fluss.option.client.security.protocol` は `client.security.protocol` を設定します。 |
| `table.datalake.paimon.<paimon_option>` | いいえ | StarRocks が Paimon を介して Fluss テーブルの lake storage にアクセスするためのオプション。 |

### 例

次の例では、Paimon filesystem metastore と S3 storage を使用する Fluss catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG fluss_catalog
PROPERTIES
(
    "type" = "fluss",
    "bootstrap.servers" = "fluss-host:9123",

    -- オプション。Fluss クライアント認証が有効な場合に設定します。
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

    -- Alibaba Cloud OSS を使用する場合は、上記の S3 warehouse と
    -- プロパティを次の設定に置き換えます。
    -- "table.datalake.paimon.warehouse" = "oss://<bucket>/<path>",
    -- "table.datalake.paimon.fs.oss.endpoint" = "oss-cn-hangzhou.aliyuncs.com",
    -- "table.datalake.paimon.fs.oss.accessKeyId" = "<access_key_id>",
    -- "table.datalake.paimon.fs.oss.accessKeySecret" = "<access_key_secret>"
);
```

## Fluss catalog の表示

[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用して、現在の StarRocks クラスター内のすべての catalog を表示できます。

```SQL
SHOW CATALOGS;
```

[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用して、External Catalog の作成ステートメントを表示することもできます。

```SQL
SHOW CREATE CATALOG fluss_catalog;
```

## Fluss catalog の削除

[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用して External Catalog を削除できます。

```SQL
DROP CATALOG fluss_catalog;
```

## Fluss テーブルのスキーマの表示

次のいずれかの構文を使用して Fluss テーブルのスキーマを表示できます。

- スキーマの表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Fluss テーブルのクエリ

1. [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用して Fluss 内のデータベースを表示します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用して、現在のセッションを Fluss catalog に切り替えます。

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   次に、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用して現在のデータベースを指定します。

   ```SQL
   USE <db_name>;
   ```

   Fluss catalog 内の現在のデータベースを直接指定することもできます。

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) を使用して Fluss テーブルをクエリします。

   ```SQL
   -- Union read: lake の履歴データと Fluss のリアルタイムデータ。
   SELECT * FROM <table_name>;

   -- Lake read: lake の履歴データのみ。
   SELECT * FROM <table_name>$lake;

   -- Real-time read: Fluss のリアルタイムデータのみ。
   SELECT * FROM <table_name>$rt;
   ```
