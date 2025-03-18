---
displayed_sidebar: docs
---

# DESC

## 説明

このステートメントを使用して、次の操作を実行できます。

- StarRocks クラスターに保存されているテーブルのスキーマを表示し、テーブルの[ソートキー](../../../table_design/indexes/Prefix_index_sort_key.md)と[マテリアライズドビュー](../../../using_starrocks/async_mv/Materialized_view.md)のタイプを確認します。
- Apache Hive™ などの外部データソースに保存されているテーブルのスキーマを表示します。この操作は、StarRocks 2.4 以降のバージョンでのみ実行できます。

## 構文

```SQL
DESC[RIBE] { [[<catalog_name>.]<db_name>.]<table_name> [ALL] | FILES(files_loading_properties) }
```

## パラメーター

| **パラメーター** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | いいえ           | 内部カタログまたは外部カタログの名前。 <ul><li>パラメーターの値を内部カタログの名前、つまり `default_catalog` に設定すると、StarRocks クラスターに保存されているテーブルのスキーマを表示できます。</li><li>パラメーターの値を外部カタログの名前に設定すると、外部データソースに保存されているテーブルのスキーマを表示できます。</li></ul> |
| db_name       | いいえ           | データベース名。                                           |
| table_name    | はい          | テーブル名。                                              |
| ALL           | いいえ           | <ul><li>このキーワードを指定すると、StarRocks クラスターに保存されているテーブルのソートキー、マテリアライズドビュー、およびスキーマのタイプを表示できます。このキーワードを指定しない場合は、テーブルスキーマのみを表示します。</li><li>外部データソースに保存されているテーブルのスキーマを表示する場合は、このキーワードを指定しないでください。</li></ul> |
| FILES         | いいえ           | FILES() テーブル関数。バージョン 3.3.4 以降では、FILES() を使用してリモートストレージに保存されているファイルのスキーマ情報を表示できます。詳細については、[関数リファレンス - FILES()](../../sql-functions/table-functions/files.md) を参照してください。 |

## 出力

```Plain
+-----------+---------------+-------+------+------+-----+---------+-------+
| IndexName | IndexKeysType | Field | Type | Null | Key | Default | Extra |
+-----------+---------------+-------+------+------+-----+---------+-------+
```

このステートメントによって返されるパラメーターを次の表に示します。

| **パラメーター** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| IndexName     | テーブル名。外部データソースに保存されているテーブルのスキーマを表示する場合、このパラメーターは返されません。 |
| IndexKeysType | テーブルのソートキーのタイプ。外部データソースに保存されているテーブルのスキーマを表示する場合、このパラメーターは返されません。 |
| Field         | カラム名。                                             |
| Type          | カラムのデータ型。                                 |
| Null          | カラムの値が NULL 可能かどうか。 <ul><li>`yes`: 値が NULL 可能であることを示します。</li><li>`no`: 値が NULL ではないことを示します。</li></ul>|
| Key           | カラムがソートキーとして使用されているかどうか。 <ul><li>`true`: カラムがソートキーとして使用されていることを示します。</li><li>`false`: カラムがソートキーとして使用されていないことを示します。</li></ul>|
| Default       | カラムのデータ型のデフォルト値。データ型にデフォルト値がない場合、NULL が返されます。 |
| Extra         | <ul><li>StarRocks クラスターに保存されているテーブルのスキーマを表示する場合、このフィールドはカラムに関する次の情報を表示します。 <ul><li>`SUM` や `MIN` など、カラムに使用されている集計関数。</li><li>カラムにブルームフィルターインデックスが作成されているかどうか。作成されている場合、`Extra` の値は `BLOOM_FILTER` です。</li></ul></li><li>外部データソースに保存されているテーブルのスキーマを表示する場合、このフィールドはカラムがパーティションカラムであるかどうかを表示します。パーティションカラムである場合、`Extra` の値は `partition key` です。</li></ul>|

> 注: マテリアライズドビューが出力にどのように表示されるかについては、例 2 を参照してください。

## 例

例 1: StarRocks クラスターに保存されている `example_table` のスキーマを表示します。

```SQL
DESC example_table;
```

または

```SQL
DESC default_catalog.example_db.example_table;
```

前述のステートメントの出力は次のとおりです。

```Plain
+-------+---------------+------+-------+---------+-------+
| Field | Type          | Null | Key   | Default | Extra |
+-------+---------------+------+-------+---------+-------+
| k1    | TINYINT       | Yes  | true  | NULL    |       |
| k2    | DECIMAL(10,2) | Yes  | true  | 10.5    |       |
| k3    | CHAR(10)      | Yes  | false | NULL    |       |
| v1    | INT           | Yes  | false | NULL    |       |
+-------+---------------+------+-------+---------+-------+
```

例 2: StarRocks クラスターに保存されている `sales_records` のスキーマ、ソートキーのタイプ、およびマテリアライズドビューを表示します。次の例では、`sales_records` に基づいて 1 つのマテリアライズドビュー `store_amt` が作成されています。

```Plain
DESC db1.sales_records ALL;

+---------------+---------------+-----------+--------+------+-------+---------+-------+
| IndexName     | IndexKeysType | Field     | Type   | Null | Key   | Default | Extra |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
| sales_records | DUP_KEYS      | record_id | INT    | Yes  | true  | NULL    |       |
|               |               | seller_id | INT    | Yes  | true  | NULL    |       |
|               |               | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_date | DATE   | Yes  | false | NULL    | NONE  |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
|               |               |           |        |      |       |         |       |
| store_amt     | AGG_KEYS      | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | SUM   |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
```

例 3: Hive クラスターに保存されている `hive_table` のスキーマを表示します。

```Plain
DESC hive_catalog.hive_db.hive_table;

+-------+----------------+------+-------+---------+---------------+ 
| Field | Type           | Null | Key   | Default | Extra         | 
+-------+----------------+------+-------+---------+---------------+ 
| id    | INT            | Yes  | false | NULL    |               | 
| name  | VARCHAR(65533) | Yes  | false | NULL    |               | 
| date  | DATE           | Yes  | false | NULL    | partition key | 
+-------+----------------+------+-------+---------+---------------+
```

例 4: AWS S3 に保存されている parquet ファイル `lineorder` のスキーマを表示します。

> **注**
>
> リモートストレージに保存されているファイルの場合、DESC は `Field`、`Type`、`Null` の 3 つのフィールドのみを返します。

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

## 参考文献

- [CREATE DATABASE](../Database/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](../Database/SHOW_CREATE_DATABASE.md)
- [USE](../Database/USE.md)
- [SHOW DATABASES](../Database/SHOW_DATABASES.md)
- [DROP DATABASE](../Database/DROP_DATABASE.md)