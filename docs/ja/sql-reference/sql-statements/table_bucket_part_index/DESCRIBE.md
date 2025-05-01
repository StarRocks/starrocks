---
displayed_sidebar: docs
---

# DESC

## Description

このステートメントを使用して、次の操作を実行できます。

- StarRocks クラスターに保存されているテーブルのスキーマを表示し、テーブルの [ソートキー](../../../table_design/Sort_key.md) と [マテリアライズドビュー](../../../using_starrocks/async_mv/Materialized_view.md) のタイプを確認します。
- Apache Hive™ などの外部データソースに保存されているテーブルのスキーマを表示します。この操作は StarRocks 2.4 以降のバージョンでのみ実行できます。

## Syntax

```SQL
DESC[RIBE] [catalog_name.][db_name.]table_name [ALL];
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | No           | 内部 catalog または外部 catalog の名前。 <ul><li>パラメータの値を内部 catalog の名前、つまり `default_catalog` に設定すると、StarRocks クラスターに保存されているテーブルのスキーマを表示できます。 </li><li>パラメータの値を外部 catalog の名前に設定すると、外部データソースに保存されているテーブルのスキーマを表示できます。</li></ul> |
| db_name       | No           | データベース名。                                           |
| table_name    | Yes          | テーブル名。                                              |
| ALL           | No           | <ul><li>このキーワードを指定すると、StarRocks クラスターに保存されているテーブルのソートキー、マテリアライズドビュー、スキーマのタイプを表示できます。このキーワードを指定しない場合は、テーブルスキーマのみを表示します。 </li><li>外部データソースに保存されているテーブルのスキーマを表示する場合、このキーワードを指定しないでください。</li></ul> |

## Output

```Plain
+-----------+---------------+-------+------+------+-----+---------+-------+
| IndexName | IndexKeysType | Field | Type | Null | Key | Default | Extra |
+-----------+---------------+-------+------+------+-----+---------+-------+
```

このステートメントによって返されるパラメータを次の表で説明します。

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| IndexName     | テーブル名。外部データソースに保存されているテーブルのスキーマを表示する場合、このパラメータは返されません。 |
| IndexKeysType | テーブルのソートキーのタイプ。外部データソースに保存されているテーブルのスキーマを表示する場合、このパラメータは返されません。 |
| Field         | カラム名。                                             |
| Type          | カラムのデータ型。                                 |
| Null          | カラムの値が NULL 可能かどうか。 <ul><li>`yes`: 値が NULL 可能であることを示します。 </li><li>`no`: 値が NULL ではないことを示します。 </li></ul>|
| Key           | カラムがソートキーとして使用されているかどうか。 <ul><li>`true`: カラムがソートキーとして使用されていることを示します。 </li><li>`false`: カラムがソートキーとして使用されていないことを示します。 </li></ul>|
| Default       | カラムのデータ型のデフォルト値。データ型にデフォルト値がない場合は、NULL が返されます。 |
| Extra         | <ul><li>StarRocks クラスターに保存されているテーブルのスキーマを表示する場合、このフィールドはカラムに関する次の情報を表示します: <ul><li>カラムで使用される集計関数、例えば `SUM` や `MIN`。 </li><li>ブルームフィルターインデックスがカラムに作成されているかどうか。作成されている場合、`Extra` の値は `BLOOM_FILTER` です。 </li></ul></li><li>外部データソースに保存されているテーブルのスキーマを表示する場合、このフィールドはカラムがパーティションカラムであるかどうかを表示します。カラムがパーティションカラムである場合、`Extra` の値は `partition key` です。 </li></ul>|

> Note: 出力におけるマテリアライズドビューの表示方法については、Example 2 を参照してください。

## Examples

Example 1: StarRocks クラスターに保存されている `example_table` のスキーマを表示します。

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

Example 2: StarRocks クラスターに保存されている `sales_records` のスキーマ、ソートキーのタイプ、およびマテリアライズドビューを表示します。次の例では、`sales_records` に基づいて 1 つのマテリアライズドビュー `store_amt` が作成されています。

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

Example 3: Hive クラスターに保存されている `hive_table` のスキーマを表示します。

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

## References

- [CREATE DATABASE](../Database/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](../Database/SHOW_CREATE_DATABASE.md)
- [USE](../Database/USE.md)
- [SHOW DATABASES](../Database/SHOW_DATABASES.md)
- [DROP DATABASE](../Database/DROP_DATABASE.md)