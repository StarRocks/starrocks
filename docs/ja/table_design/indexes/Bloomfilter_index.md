---
displayed_sidebar: docs
sidebar_position: 30
---

# ブルームフィルターインデックス

このトピックでは、ブルームフィルターインデックスの作成と変更方法、およびその動作について説明します。

ブルームフィルターインデックスは、テーブルのデータファイル内のフィルタリングされたデータの存在の可能性を検出するために使用される、空間効率の高いデータ構造です。ブルームフィルターインデックスがフィルタリング対象のデータが特定のデータファイルに存在しないことを検出した場合、StarRocks はそのデータファイルのスキャンをスキップします。ブルームフィルターインデックスは、列（例えば ID）のカーディナリティが比較的高い場合に応答時間を短縮することができます。

クエリがソートキー列にヒットした場合、StarRocks は [プレフィックスインデックス](./Prefix_index_sort_key.md) を使用して効率的にクエリ結果を返します。ただし、データブロックのプレフィックスインデックスエントリは36バイトを超えることはできません。ソートキーとして使用されておらず、カーディナリティが比較的高い列のクエリパフォーマンスを向上させたい場合、その列にブルームフィルターインデックスを作成することができます。

## 動作の仕組み

例えば、特定のテーブル `table1` の `column1` にブルームフィルターインデックスを作成し、`Select xxx from table1 where column1 = something;` のようなクエリを実行すると、StarRocks が `table1` のデータファイルをスキャンする際に次のような状況が発生します。

- ブルームフィルターインデックスがデータファイルにフィルタリング対象のデータが含まれていないことを検出した場合、StarRocks はクエリパフォーマンスを向上させるためにそのデータファイルをスキップします。
- ブルームフィルターインデックスがデータファイルにフィルタリング対象のデータが含まれている可能性があることを検出した場合、StarRocks はデータファイルを読み込み、データが存在するかどうかを確認します。ブルームフィルターは、値が存在しないことを確実に教えてくれますが、値が存在することを確実に教えてくれるわけではなく、存在する可能性があることしか教えてくれません。ブルームフィルターインデックスを使用して値が存在するかどうかを判断すると、偽陽性が発生する可能性があります。つまり、ブルームフィルターインデックスがデータファイルにフィルタリング対象のデータが含まれていると検出しても、実際にはデータファイルにデータが含まれていない場合があります。

## 使用上の注意

- 重複キーまたは主キーテーブルのすべての列にブルームフィルターインデックスを作成できます。集計テーブルまたはユニークキーテーブルの場合、キー列にのみブルームフィルターインデックスを作成できます。
- ブルームフィルターインデックスは、以下のデータ型の列に対して作成できます:
  - 数値型: SMALLINT, INT, BIGINT, LARGEINT
  - 文字列型: CHAR, STRING, VARCHAR
  - 日付型: DATE, DATETIME
- ブルームフィルターインデックスは、`in` および `=` 演算子を含むクエリのパフォーマンスを向上させることができます。例えば、`Select xxx from table where x in {}` や `Select xxx from table where column = xxx` のようなクエリです。
- クエリがブルームフィルターインデックスを使用しているかどうかは、クエリのプロファイルの `BloomFilterFilterRows` フィールドを確認することで確認できます。

## ブルームフィルターインデックスの作成

テーブルを作成する際に `PROPERTIES` の `bloom_filter_columns` パラメータを指定することで、列に対してブルームフィルターインデックスを作成できます。例えば、`table1` の `k1` および `k2` 列にブルームフィルターインデックスを作成します。

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

複数の列に対して一度にブルームフィルターインデックスを作成することができます。これらの列名を指定する際には、カンマ（`,`）で区切る必要があります。CREATE TABLE 文の他のパラメータの説明については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

## ブルームフィルターインデックスの表示

例えば、次の文は `table1` のブルームフィルターインデックスを表示します。出力の説明については、[SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) を参照してください。

```SQL
SHOW CREATE TABLE table1;
```

## ブルームフィルターインデックスの変更

[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 文を使用して、ブルームフィルターインデックスを追加、削減、削除することができます。

- 次の文は、`v1` 列にブルームフィルターインデックスを追加します。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1,k2,v1");
    ```

- 次の文は、`k2` 列のブルームフィルターインデックスを削減します。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1");
    ```

- 次の文は、`table1` のすべてのブルームフィルターインデックスを削除します。

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "");
    ```

> 注意: インデックスの変更は非同期操作です。この操作の進行状況は、[SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) を実行することで確認できます。テーブルごとに一度に1つのインデックス変更タスクしか実行できません。