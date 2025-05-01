---
displayed_sidebar: docs
---

# unnest

## 説明

UNNESTは、配列を受け取り、その配列内の要素をテーブルの複数の行に変換するテーブル関数です。この変換は「フラット化」とも呼ばれます。

Lateral Join を使用して UNNEST を用いることで、STRING、ARRAY、または BITMAP から複数の行への一般的な変換を実装できます。詳細は [Lateral join](../../../using_starrocks/Lateral_join.md) を参照してください。

バージョン 2.5 から、UNNEST は可変数の配列パラメータを受け取ることができます。配列は型や長さ（要素数）が異なる場合があります。配列の長さが異なる場合、最も長い長さが優先され、この長さに満たない配列には null が追加されます。詳細は [Example 2](#example-2-unnest-takes-multiple-parameters) を参照してください。

## 構文

```Haskell
unnest(array0[, array1 ...])
```

## パラメータ

`array`: 変換したい配列です。これは ARRAY データ型に評価できる配列または式でなければなりません。1つ以上の配列または配列式を指定できます。

## 戻り値

配列から変換された複数の行を返します。戻り値の型は、配列内の要素の型に依存します。

配列でサポートされている要素の型については、[ARRAY](../../sql-statements/data-types/Array.md) を参照してください。

## 使用上の注意

- UNNEST はテーブル関数です。Lateral Join と共に使用する必要がありますが、キーワード Lateral Join を明示的に指定する必要はありません。
- 配列式が NULL に評価されるか、空の場合、行は返されません。
- 配列内の要素が NULL の場合、その要素には NULL が返されます。

## 例

### 例 1: UNNEST が1つのパラメータを受け取る場合

```SQL
-- scores が ARRAY 列である student_score テーブルを作成します。
CREATE TABLE student_score
(
    `id` bigint(20) NULL COMMENT "",
    `scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`) BUCKETS 1;

-- このテーブルにデータを挿入します。
INSERT INTO student_score VALUES
(1, [80,85,87]),
(2, [77, null, 89]),
(3, null),
(4, []),
(5, [90,92]);

-- このテーブルからデータをクエリします。
SELECT * FROM student_score ORDER BY id;
+------+--------------+
| id   | scores       |
+------+--------------+
|    1 | [80,85,87]   |
|    2 | [77,null,89] |
|    3 | NULL         |
|    4 | []           |
|    5 | [90,92]      |
+------+--------------+

-- UNNEST を使用して scores 列を複数の行にフラット化します。
SELECT id, scores, unnest FROM student_score, unnest(scores) AS unnest;
+------+--------------+--------+
| id   | scores       | unnest |
+------+--------------+--------+
|    1 | [80,85,87]   |     80 |
|    1 | [80,85,87]   |     85 |
|    1 | [80,85,87]   |     87 |
|    2 | [77,null,89] |     77 |
|    2 | [77,null,89] |   NULL |
|    2 | [77,null,89] |     89 |
|    5 | [90,92]      |     90 |
|    5 | [90,92]      |     92 |
+------+--------------+--------+
```

`id = 1` に対応する [80,85,87] は3行に変換されます。

`id = 2` に対応する [77,null,89] は null 値を保持します。

`id = 3` および `id = 4` に対応する `scores` は NULL および空であり、スキップされます。

### 例 2: UNNEST が複数のパラメータを受け取る場合

```SQL
-- type および scores 列が異なる型を持つ example_table テーブルを作成します。
CREATE TABLE example_table (
id varchar(65533) NULL COMMENT "",
type varchar(65533) NULL COMMENT "",
scores ARRAY<int> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
"replication_num" = "3");

-- テーブルにデータを挿入します。
INSERT INTO example_table VALUES
("1", "typeA;typeB", [80,85,88]),
("2", "typeA;typeB;typeC", [87,90,95]);

-- テーブルからデータをクエリします。
SELECT * FROM example_table;
+------+-------------------+------------+
| id   | type              | scores     |
+------+-------------------+------------+
| 1    | typeA;typeB       | [80,85,88] |
| 2    | typeA;typeB;typeC | [87,90,95] |
+------+-------------------+------------+

-- UNNEST を使用して type と scores を複数の行に変換します。
SELECT id, unnest.type, unnest.scores
FROM example_table, unnest(split(type, ";"), scores) AS unnest(type,scores);
+------+-------+--------+
| id   | type  | scores |
+------+-------+--------+
| 1    | typeA |     80 |
| 1    | typeB |     85 |
| 1    | NULL  |     88 |
| 2    | typeA |     87 |
| 2    | typeB |     90 |
| 2    | typeC |     95 |
+------+-------+--------+
```

`UNNEST` 内の `type` と `scores` は型と長さが異なります。

`type` は VARCHAR 列であり、`scores` は ARRAY 列です。split() 関数は `type` を ARRAY に変換するために使用されます。

`id = 1` の場合、`type` は ["typeA","typeB"] に変換され、2つの要素を持ちます。

`id = 2` の場合、`type` は ["typeA","typeB","typeC"] に変換され、3つの要素を持ちます。

各 `id` に対して行数を一致させるために、["typeA","typeB"] に null 要素が追加されます。