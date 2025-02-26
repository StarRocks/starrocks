---
displayed_sidebar: docs
---

# [実験的機能] Kudu catalog

StarRocks は v3.3 以降で Kudu catalog をサポートしています。

Kudu catalog は、Apache Kudu からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Kudu catalog を基に [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、Kudu からデータを直接変換してロードすることもできます。

Kudu クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが以下の重要なコンポーネントと統合する必要があります。

- Kudu ファイルシステムや Hive メタストアのようなメタストア

## 使用上の注意

Kudu catalog はデータのクエリにのみ使用できます。Kudu catalog を使用して Kudu クラスター内のデータを削除、削除、または挿入することはできません。

## 統合準備

Kudu catalog を作成する前に、StarRocks クラスターが Kudu クラスターのストレージシステムとメタストアと統合できることを確認してください。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合は、KUDU クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

KUDU クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように設定します。

- 各 FE と各 BE で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、KUDU クラスターと Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感です。そのため、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルと各 BE の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Kudu catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "kudu",
    CatalogParams
)
```

### パラメータ

#### catalog_name

Kudu catalog の名前です。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えてはなりません。

#### comment

Kudu catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `kudu` に設定します。

#### CatalogParams

StarRocks が Kudu クラスターのメタデータにアクセスする方法に関するパラメータのセットです。

以下の表は、`CatalogParams` で設定する必要があるパラメータを説明しています。

| パラメータ           | 必須 | 説明                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kudu.catalog.type   | はい      | Kudu クラスターで使用するメタストアのタイプです。このパラメータを `kudu` または `hive` に設定します。                                                                                                                                                                                                                                                                                                                       |
| kudu.master                   | いいえ       | Kudu マスターのアドレスを指定します。デフォルトは `localhost:7051` です。                                                                                                                                                                                                                                                                                                                                         |
| hive.metastore.uris | いいえ       | Hive メタストアの URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。Hive メタストアで高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |
| kudu.schema-emulation.enabled | いいえ       | `schema` エミュレーションを有効または無効にするオプションです。デフォルトではオフ (false) になっており、すべてのテーブルは `default` `schema` に属します。                                                                                                                                                                                                                                                                 |
| kudu.schema-emulation.prefix | いいえ       | `schema` エミュレーションのプレフィックスは、`kudu.schema-emulation.enabled` = `true` の場合にのみ設定する必要があります。デフォルトのプレフィックスは空文字列です: ` `。                                                                                                                                                                                                                                                                       |

> **注意**
>
> Hive メタストアを使用する場合、Kudu データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始した際に StarRocks が Hive メタストアにアクセスできない可能性があります。

### 例

- 次の例は、Kudu クラスターからデータをクエリするために、メタストアタイプ `kudu.catalog.type` が `kudu` に設定された `kudu_catalog` という名前の Kudu catalog を作成します。

  ```SQL
  CREATE EXTERNAL CATALOG kudu_catalog
  PROPERTIES
  (
      "type" = "kudu",
      "kudu.master" = "localhost:7051",
      "kudu.catalog.type" = "kudu",
      "kudu.schema-emulation.enabled" = "true",
      "kudu.schema-emulation.prefix" = "impala::"
  );
  ```

- 次の例は、Kudu クラスターからデータをクエリするために、メタストアタイプ `kudu.catalog.type` が `hive` に設定された `kudu_catalog` という名前の Kudu catalog を作成します。

  ```SQL
  CREATE EXTERNAL CATALOG kudu_catalog
  PROPERTIES
  (
      "type" = "kudu",
      "kudu.master" = "localhost:7051",
      "kudu.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "kudu.schema-emulation.enabled" = "true",
      "kudu.schema-emulation.prefix" = "impala::"
  );
  ```

## Kudu catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。次の例では、`kudu_catalog` という名前の Kudu catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG kudu_catalog;
```

## Kudu catalog の削除

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

次の例では、`kudu_catalog` という名前の Kudu catalog を削除します。

```SQL
DROP Catalog kudu_catalog;
```

## Kudu テーブルのスキーマを表示

Kudu テーブルのスキーマを表示するには、次の構文のいずれかを使用します。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Kudu テーブルのクエリ

1. Kudu クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 現在のセッションで目的の catalog に切り替えるには、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用します。

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   次に、現在のセッションでアクティブなデータベースを指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

   ```SQL
   USE <db_name>;
   ```

   または、目的の catalog でアクティブなデータベースを直接指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Kudu からのデータのロード

OLAP テーブル `olap_tbl` があると仮定して、以下のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM kudu_table;
```