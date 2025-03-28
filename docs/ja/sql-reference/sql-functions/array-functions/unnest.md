---
displayed_sidebar: docs
---

# unnest

UNNEST は、配列を受け取り、その配列内の要素をテーブルの複数の行に変換するテーブル関数です。この変換は「フラット化」とも呼ばれます。

UNNEST と Lateral Join を組み合わせて、STRING、ARRAY、または BITMAP から複数の行への一般的な変換を実装できます。詳細は [Lateral join](../../../using_starrocks/Lateral_join.md) を参照してください。

バージョン 2.5 から、UNNEST は可変数の配列パラメータを受け取ることができます。配列は型や長さ（要素数）が異なる場合があります。配列の長さが異なる場合、最も長い長さが優先され、これより短い配列には null が追加されます。詳細は [Example 2](#example-2-unnest-takes-multiple-parameters) を参照してください。

バージョン 3.2.7 から、UNNEST は LEFT JOIN ON TRUE と共に使用でき、右テーブルの対応する行が空または null 値であっても左テーブルのすべての行を保持します。このような空または NULL の行には NULL が返されます。詳細は [Example 3](#example-3-unnest-with-left-join-on-true) を参照してください。

## Syntax

```Haskell
unnest(array0[, array1 ...])
```

## Parameters

`array`: 変換したい配列です。ARRAY データ型に評価できる配列または式でなければなりません。1 つ以上の配列または配列式を指定できます。

## Return value

配列から変換された複数の行を返します。戻り値の型は配列内の要素の型に依存します。

配列でサポートされる要素型については、[ARRAY](../../data-types/semi_structured/Array.md) を参照してください。

## Usage notes

- UNNEST はテーブル関数です。Lateral Join と共に使用する必要がありますが、キーワード Lateral Join を明示的に指定する必要はありません。
- 配列式が NULL に評価されるか、空の場合、行は返されません（LEFT JOIN ON TRUE を除く）。
- 配列内の要素が NULL の場合、その要素には NULL が返されます。

## Examples

### Example 1: UNNEST takes one parameter

```SQL
-- scores が ARRAY 列であるテーブル student_score を作成します。
CREATE TABLE student_score
(
    `id` bigint(20) NULL COMMENT "",
    `scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`);

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

`id = 1` に対応する [80,85,87] は 3 行に変換されます。

`id = 2` に対応する [77,null,89] は null 値を保持します。

`id = 3` と `id = 4` に対応する `scores` は NULL および空であり、スキップされます。

### Example 2: UNNEST takes multiple parameters

```SQL
-- type と scores 列が異なる型を持つテーブル example_table を作成します。
CREATE TABLE example_table (
id varchar(65533) NULL COMMENT "",
type varchar(65533) NULL COMMENT "",
scores ARRAY<int> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id)
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

`type` は VARCHAR 列であり、`scores` は ARRAY 列です。split() 関数を使用して `type` を ARRAY に変換します。

`id = 1` の場合、`type` は ["typeA","typeB"] に変換され、2 つの要素を持ちます。

`id = 2` の場合、`type` は ["typeA","typeB","typeC"] に変換され、3 つの要素を持ちます。

各 `id` に対して一貫した行数を確保するために、["typeA","typeB"] に null 要素が追加されます。

### Example 3: UNNEST with LEFT JOIN ON TRUE

```SQL
-- scores が ARRAY 列であるテーブル student_score を作成します。
CREATE TABLE student_score
(
`id` bigint(20) NULL COMMENT "",
`scores` ARRAY<int> NULL COMMENT ""
)
DUPLICATE KEY (id)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "1"
);

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

-- LEFT JOIN ON TRUE を使用します。
SELECT id, scores, unnest FROM student_score LEFT JOIN unnest(scores) AS unnest ON TRUE ORDER BY 1, 3;
+------+--------------+--------+
| id   | scores       | unnest |
+------+--------------+--------+
|  1   | [80,85,87]   |     80 |
|  1   | [80,85,87]   |     85 |
|  1   | [80,85,87]   |     87 |
|  2   | [77,null,89] |   NULL |
|  2   | [77,null,89] |     77 |
|  2   | [77,null,89] |     89 |
|  3   | NULL         |   NULL |
|  4   | []           |   NULL |
|  5   | [90,92]      |     90 |
|  5   | [90,92]      |     92 |
+------+--------------+--------+
```

`id = 1` に対応する [80,85,87] は 3 行に変換されます。

`id = 2` に対応する [77,null,89] の null 値は保持されます。

`id = 3` と `id = 4` に対応する `scores` は NULL および空です。Left Join はこれらの 2 行を保持し、それらに対して NULL を返します。