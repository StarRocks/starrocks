---
displayed_sidebar: docs
---

# DESC

## 説明

このステートメントを使用して、以下の操作を行うことができます。

- StarRocks クラスターに保存されているテーブルのスキーマを表示し、テーブルの [ソートキー](../../../table_design/Sort_key.md) と [マテリアライズドビュー](../../../using_starrocks/Materialized_view.md) のタイプを確認します。
- Apache Hive™ などの外部データソースに保存されているテーブルのスキーマを表示します。この操作は StarRocks 2.4 以降のバージョンでのみ実行可能です。

## 構文

```SQL
DESC[RIBE] [catalog_name.][db_name.]table_name [ALL];
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | いいえ           | 内部カタログまたは外部カタログの名前。 <ul><li>パラメータの値を内部カタログの名前 `default_catalog` に設定した場合、StarRocks クラスターに保存されているテーブルのスキーマを表示できます。 </li><li>パラメータの値を外部カタログの名前に設定した場合、外部データソースに保存されているテーブルのスキーマを表示できます。</li></ul> |
| db_name       | いいえ           | データベース名。                                           |
| table_name    | はい          | テーブル名。                                              |
| ALL           | いいえ           | <ul><li>このキーワードを指定すると、StarRocks クラスターに保存されているテーブルのソートキー、マテリアライズドビュー、およびスキーマのタイプを表示できます。このキーワードを指定しない場合、テーブルスキーマのみを表示します。 </li><li>外部データソースに保存されているテーブルのスキーマを表示する際には、このキーワードを指定しないでください。</li></ul> |

## 出力

```Plain
+-----------+---------------+-------+------+------+-----+---------+-------+
| IndexName | IndexKeysType | Field | Type | Null | Key | Default | Extra |
+-----------+---------------+-------+------+------+-----+---------+-------+
```

このステートメントによって返されるパラメータを以下の表で説明します。

| **パラメータ** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| IndexName     | テーブル名。外部データソースに保存されているテーブルのスキーマを表示する場合、このパラメータは返されません。 |
| IndexKeysType | テーブルのソートキーのタイプ。外部データソースに保存されているテーブルのスキーマを表示する場合、このパラメータは返されません。 |
| Field         | カラム名。                                             |
| Type          | カラムのデータタイプ。                                 |
| Null          | カラムの値が NULL 可能かどうか。 <ul><li>`yes`: 値が NULL 可能であることを示します。 </li><li>`no`: 値が NULL ではないことを示します。 </li></ul>|
| Key           | カラムがソートキーとして使用されているかどうか。 <ul><li>`true`: カラムがソートキーとして使用されていることを示します。 </li><li>`false`: カラムがソートキーとして使用されていないことを示します。 </li></ul>|
| Default       | カラムのデータタイプのデフォルト値。データタイプにデフォルト値がない場合、NULL が返されます。 |
| Extra         | <ul><li>StarRocks クラスターに保存されているテーブルのスキーマを表示する場合、このフィールドはカラムに関する以下の情報を表示します: <ul><li>カラムで使用される集計関数、例えば `SUM` や `MIN`。 </li><li>カラムにブルームフィルターインデックスが作成されているかどうか。作成されている場合、`Extra` の値は `BLOOM_FILTER` です。 </li></ul></li><li>外部データソースに保存されているテーブルのスキーマを表示する場合、このフィールドはカラムがパーティションカラムであるかどうかを表示します。パーティションカラムである場合、`Extra` の値は `partition key` です。 </li></ul>|

> 注: 出力にマテリアライズドビューがどのように表示されるかについては、例 2 を参照してください。

## 例

例 1: StarRocks クラスターに保存されている `example_table` のスキーマを表示します。

```SQL
DESC example_table;
```

または

```SQL
DESC default_catalog.example_db.example_table;
```

上記のステートメントの出力は以下の通りです。

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

例 2: StarRocks クラスターに保存されている `sales_records` のスキーマ、ソートキーのタイプ、およびマテリアライズドビューを表示します。以下の例では、`sales_records` に基づいて 1 つのマテリアライズドビュー `store_amt` が作成されています。

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

## 参考

- [CREATE DATABASE](../data-definition/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](../data-manipulation/SHOW_CREATE_DATABASE.md)
- [USE](../data-definition/USE.md)
- [SHOW DATABASES](../data-manipulation/SHOW_DATABASES.md)
- [DROP DATABASE](../data-definition/DROP_DATABASE.md)