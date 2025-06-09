---
displayed_sidebar: docs
sidebar_position: 30
---

# ブルームフィルターインデックス

このトピックでは、ブルームフィルターインデックスの作成と変更方法、およびその動作について説明します。

ブルームフィルターインデックスは、テーブルのデータファイル内のフィルタリングされたデータの存在の可能性を検出するために使用される、スペース効率の高いデータ構造です。ブルームフィルターインデックスがフィルタリングされるデータが特定のデータファイルに存在しないことを検出した場合、StarRocks はそのデータファイルのスキャンをスキップします。ブルームフィルターインデックスは、列（例えば ID）のカーディナリティが比較的高い場合に応答時間を短縮できます。

クエリがソートキー列にヒットした場合、StarRocks は [プレフィックスインデックス](./Prefix_index_sort_key.md) を使用して効率的にクエリ結果を返します。ただし、データブロックのプレフィックスインデックスエントリは36バイトを超えることはできません。ソートキーとして使用されておらず、カーディナリティが比較的高い列のクエリパフォーマンスを向上させたい場合、その列に対してブルームフィルターインデックスを作成できます。

## 動作の仕組み

例えば、特定のテーブル `table1` の `column1` にブルームフィルターインデックスを作成し、`Select xxx from table1 where column1 = something;` のようなクエリを実行すると、StarRocks が `table1` のデータファイルをスキャンする際に次のような状況が発生します。

- ブルームフィルターインデックスがデータファイルにフィルタリングされるデータが含まれていないことを検出した場合、StarRocks はクエリパフォーマンスを向上させるためにそのデータファイルをスキップします。
- ブルームフィルターインデックスがデータファイルにフィルタリングされるデータが含まれている可能性があることを検出した場合、StarRocks はデータが存在するかどうかを確認するためにデータファイルを読み込みます。ブルームフィルターは値が存在しないことを確実に伝えることができますが、値が存在することを確実に伝えることはできず、存在する可能性があることしか伝えられません。ブルームフィルターインデックスを使用して値が存在するかどうかを判断すると、偽陽性が発生する可能性があります。これは、ブルームフィルターインデックスがデータファイルにフィルタリングされるデータが含まれていると検出しても、実際にはデータファイルにデータが含まれていない場合を意味します。

## 使用上の注意

- 重複キーまたは主キーテーブルのすべての列に対してブルームフィルターインデックスを作成できます。集計テーブルまたはユニークキーテーブルの場合、キー列にのみブルームフィルターインデックスを作成できます。
- ブルームフィルターインデックスは、次のデータ型の列に対して作成できます。
  - 数値型: SMALLINT, INT, BIGINT, および LARGEINT。
  - 文字列型: CHAR, STRING, および VARCHAR。
  - 日付型: DATE および DATETIME。
- ブルームフィルターインデックスは、`in` および `=` 演算子を含むクエリのパフォーマンスのみを向上させることができます。例えば、`Select xxx from table where x in {}` や `Select xxx from table where column = xxx` などです。
- クエリがブルームフィルターインデックスを使用しているかどうかは、クエリのプロファイルの `BloomFilterFilterRows` フィールドを確認することで確認できます。

## ブルームフィルターインデックスの作成

テーブルを作成する際に `PROPERTIES` の `bloom_filter_columns` パラメータを指定することで、列に対してブルームフィルターインデックスを作成できます。例えば、`table1` の `k1` および `k2` 列に対してブルームフィルターインデックスを作成します。

```SQL
CREATE TABLE table1
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT DEFAULT "10"
)
ENGINE = olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES("bloom_filter_columns" = "k1,k2");
```

複数の列に対してブルームフィルターインデックスを一度に作成するには、これらの列名を指定します。これらの列名はカンマ（`,`）で区切る必要があります。CREATE TABLE ステートメントの他のパラメータの説明については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

## ブルームフィルターインデックスの表示

例えば、次のステートメントは `table1` のブルームフィルターインデックスを表示します。出力の説明については、[SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) を参照してください。

```SQL
SHOW CREATE TABLE table1;
```

## ブルームフィルターインデックスの変更

[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) ステートメントを使用して、ブルームフィルターインデックスを追加、削減、削除できます。

- 次のステートメントは、`v1` 列にブルームフィルターインデックスを追加します。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1,k2,v1");
    ```

- 次のステートメントは、`k2` 列のブルームフィルターインデックスを削減します。
  
    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1");
    ```

- 次のステートメントは、`table1` のすべてのブルームフィルターインデックスを削除します。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "");
    ```

> 注: インデックスの変更は非同期操作です。この操作の進行状況は、[SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) を実行することで確認できます。テーブルごとに一度に1つのインデックス変更タスクのみを実行できます。