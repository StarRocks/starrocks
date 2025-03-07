---
displayed_sidebar: docs
---

# JSON

StarRocks は v2.2.0 から JSON データ型をサポートしています。このトピックでは、JSON の基本概念について説明します。また、JSON カラムの作成方法、JSON データのロード方法、JSON データのクエリ方法、および JSON 関数と演算子を使用して JSON データを構築および処理する方法についても説明します。

## JSON とは

JSON は、半構造化データ用に設計された軽量のデータ交換フォーマットです。JSON はデータを階層的なツリー構造で表現し、さまざまなデータストレージおよび分析シナリオで柔軟かつ読み書きが容易です。JSON は `NULL` 値と次のデータ型をサポートします: NUMBER、STRING、BOOLEAN、ARRAY、および OBJECT。

JSON についての詳細は、[JSON website](https://www.json.org/json-en.html) をご覧ください。JSON の入力および出力構文については、[RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4) の JSON 仕様を参照してください。

StarRocks は、JSON データのストレージと効率的なクエリおよび分析の両方をサポートしています。StarRocks は入力テキストを直接保存するのではなく、JSON データをバイナリ形式で保存し、解析コストを削減し、クエリ効率を向上させます。

## JSON データの使用

### JSON カラムの作成

テーブルを作成する際に、`JSON` キーワードを使用して `j` カラムを JSON カラムとして指定できます。

```sql
CREATE TABLE `tj` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  JSON NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);
```

### データのロードと JSON データとしての保存

StarRocks は、データをロードして JSON データとして保存するための以下の方法を提供しています。

- 方法 1: `INSERT INTO` を使用して、テーブルの JSON カラムにデータを書き込みます。以下の例では、`tj` という名前のテーブルが使用され、そのテーブルの `j` カラムは JSON カラムです。

```plaintext
INSERT INTO tj (id, j) VALUES (1, parse_json('{"a": 1, "b": true}'));
INSERT INTO tj (id, j) VALUES (2, parse_json('{"a": 2, "b": false}'));
INSERT INTO tj (id, j) VALUES (3, parse_json('{"a": 3, "b": true}'));
INSERT INTO tj (id, j) VALUES (4, json_object('a', 4, 'b', false)); 
```

> parse_json 関数は STRING データを JSON データとして解釈できます。json_object 関数は JSON オブジェクトを構築したり、既存のテーブルを JSON ファイルに変換したりできます。詳細は [parse_json](../../sql-functions/json-functions/json-constructor-functions/parse_json.md) と [json_object](../../sql-functions/json-functions/json-constructor-functions/json_object.md) を参照してください。

- 方法 2: Stream Load を使用して JSON ファイルをロードし、そのファイルを JSON データとして保存します。詳細は [Load JSON data](../../../loading/StreamLoad.md#load-json-data) を参照してください。

  - ルート JSON オブジェクトをロードしたい場合は、`jsonpaths` を `$` に設定します。
  - JSON オブジェクトの特定の値をロードしたい場合は、`jsonpaths` を `$.a` に設定します。ここで `a` はキーを指定します。StarRocks でサポートされている JSON パス式の詳細は [JSON path](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md#json-path-expressions) を参照してください。

- 方法 3: Broker Load を使用して Parquet ファイルをロードし、そのファイルを JSON データとして保存します。詳細は [Broker Load](../../sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

StarRocks は、Parquet ファイルのロード時に以下のデータ型変換をサポートしています。

| Parquet ファイルのデータ型                                    | JSON データ型 |
| ------------------------------------------------------------ | -------------- |
| INTEGER (INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, and UINT64) | NUMBER         |
| FLOAT and DOUBLE                                             | NUMBER         |
| BOOLEAN                                                      | BOOLEAN        |
| STRING                                                       | STRING         |
| MAP                                                          | OBJECT         |
| STRUCT                                                       | OBJECT         |
| LIST                                                         | ARRAY          |
| Other data types such as UNION and TIMESTAMP                 | Not supported  |

- 方法 4: [Routine](../../../loading/Loading_intro.md) load を使用して、Kafka から StarRocks に JSON データを継続的にロードします。

### JSON データのクエリと処理

StarRocks は JSON データのクエリと処理、および JSON 関数と演算子の使用をサポートしています。

以下の例では、`tj` という名前のテーブルが使用され、そのテーブルの `j` カラムは JSON カラムとして指定されています。

```plaintext
mysql> select * from tj;
+------+----------------------+
| id   |          j           |
+------+----------------------+
| 1    | {"a": 1, "b": true}  |
| 2    | {"a": 2, "b": false} |
| 3    | {"a": 3, "b": true}  |
| 4    | {"a": 4, "b": false} |
+------+----------------------+
```

例 1: JSON カラムのデータをフィルタリングして、`id=1` の条件を満たすデータを取得します。

```plaintext
mysql> select * from tj where id = 1;
+------+---------------------+
| id   |           j         |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
+------+---------------------+
```

例 2: JSON カラム `j` のデータをフィルタリングして、指定された条件を満たすデータを取得します。

> `j->'a'` は JSON データを返します。データを比較するために最初の例を使用できます（この例では暗黙の変換が行われます）。または、CAST 関数を使用して JSON データを INT に変換し、その後データを比較することもできます。

```plaintext
mysql> select * from tj where j->'a' = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |


mysql> select * from tj where cast(j->'a' as INT) = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |
+------+---------------------+
```

例 3: CAST 関数を使用して、テーブルの JSON カラムの値を BOOLEAN 値に変換します。その後、JSON カラムのデータをフィルタリングして、指定された条件を満たすデータを取得します。

```plaintext
mysql> select * from tj where cast(j->'b' as boolean);
+------+---------------------+
|  id  |          j          |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
| 3    | {"a": 3, "b": true} |
+------+---------------------+
```

例 4: CAST 関数を使用して、テーブルの JSON カラムの値を BOOLEAN 値に変換します。その後、JSON カラムのデータをフィルタリングして、指定された条件を満たすデータを取得し、データに対して算術演算を行います。

```plaintext
mysql> select cast(j->'a' as int) from tj where cast(j->'b' as boolean);
+-----------------------+
|  CAST(j->'a' AS INT)  |
+-----------------------+
|          3            |
|          1            |
+-----------------------+

mysql> select sum(cast(j->'a' as int)) from tj where cast(j->'b' as boolean);
+----------------------------+
| sum(CAST(j->'a' AS INT))  |
+----------------------------+
|              4             |
+----------------------------+
```

例 5: JSON カラムをソートキーとして使用して、テーブルのデータをソートします。

```plaintext
mysql> select * from tj
    ->        where j->'a' <= 3
    ->        order by cast(j->'a' as int);
+------+----------------------+
| id   | j                    |
+------+----------------------+
|    1 | {"a": 1, "b": true}  |
|    2 | {"a": 2, "b": false} |
|    3 | {"a": 3, "b": true}  |
|    4 | {"a": 4, "b": false} |
+------+----------------------+
4 rows in set (0.05 sec)
```

## JSON 関数と演算子

JSON 関数と演算子を使用して、JSON データを構築および処理できます。詳細は [Overview of JSON functions and operators](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md) を参照してください。

## 制限事項と使用上の注意

- JSON 値の最大長は 16 MB です。

- ORDER BY、GROUP BY、および JOIN 句は JSON カラムへの参照をサポートしていません。JSON カラムへの参照を作成したい場合は、CAST 関数を使用して JSON カラムを SQL カラムに変換してから参照を作成してください。詳細は [cast](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md) を参照してください。

- JSON カラムは Duplicate Key、Primary Key、および Unique Key テーブルでサポートされています。集計テーブルではサポートされていません。

- JSON カラムは、DUPLICATE KEY、PRIMARY KEY、および UNIQUE KEY テーブルのパーティションキー、バケッティングキー、またはディメンションカラムとして使用できません。また、ORDER BY、GROUP BY、および JOIN 句でも使用できません。

- StarRocks は、JSON データをクエリするために次の JSON 比較演算子を使用することを許可しています: `<`, `<=`, `>`, `>=`, `=`, および `!=`。`IN` を使用して JSON データをクエリすることはできません。