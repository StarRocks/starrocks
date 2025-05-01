---
displayed_sidebar: docs
---

# DELETE

指定された条件に基づいてテーブルからデータ行を削除します。テーブルはパーティション化されている場合とされていない場合があります。

重複キーテーブル、集計テーブル、およびユニークキーテーブルでは、指定されたパーティションからデータを削除できます。v2.3から、主キーテーブルは完全な `DELETE...WHERE` セマンティクスをサポートし、主キー、任意のカラム、またはサブクエリの結果に基づいてデータ行を削除できます。v3.0から、StarRocks は `DELETE...WHERE` セマンティクスをマルチテーブルジョインや共通テーブル式 (CTE) で強化しています。データベース内の他のテーブルと主キーテーブルをジョインする必要がある場合、USING 句や CTE でこれらの他のテーブルを参照できます。

## 使用上の注意

- DELETE を実行したいテーブルとデータベースに対する権限を持っている必要があります。
- 頻繁な DELETE 操作は推奨されません。必要な場合は、ピーク時以外に実行してください。
- DELETE 操作はテーブル内のデータのみを削除します。テーブル自体は残ります。テーブルを削除するには、[DROP TABLE](DROP_TABLE.md) を実行してください。
- 誤操作でテーブル全体のデータが削除されるのを防ぐため、DELETE ステートメントで WHERE 句を指定する必要があります。
- 削除された行はすぐにはクリーンアップされません。それらは「削除済み」とマークされ、セグメントに一時的に保存されます。物理的には、データバージョンのマージ (コンパクション) が完了した後にのみ行が削除されます。
- この操作は、このテーブルを参照するマテリアライズドビューのデータも削除します。

## 重複キーテーブル、集計テーブル、およびユニークキーテーブル

### 構文

```SQL
DELETE FROM  [<db_name>.]<table_name> [PARTITION <partition_name>]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...]
```

### パラメータ

| **パラメータ**         | **必須** | **説明**                                                     |
| :--------------- | :------- | :----------------------------------------------------------- |
| `db_name`        | いいえ       | 対象テーブルが属するデータベース。このパラメータが指定されていない場合、デフォルトで現在のデータベースが使用されます。      |
| `table_name`     | はい      | データを削除したいテーブル。   |
| `partition_name` | いいえ       | データを削除したいパーティション。  |
| `column_name`    | はい      | DELETE 条件として使用したいカラム。1つ以上のカラムを指定できます。   |
| `op`             | はい      | DELETE 条件で使用される演算子。サポートされている演算子は `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, および `NOT IN` です。 |

### 制限と使用上の注意

- 重複キーテーブルでは、**任意のカラム** を DELETE 条件として使用できます。集計テーブルおよびユニークキーテーブルでは、**キー カラム** のみが DELETE 条件として使用できます。

- 指定する条件は AND 関係でなければなりません。OR 関係で条件を指定したい場合は、別々の DELETE ステートメントで条件を指定する必要があります。

- 重複キーテーブル、集計テーブル、およびユニークキーテーブルでは、DELETE ステートメントでサブクエリの結果を条件として使用することはサポートされていません。

### 影響

DELETE ステートメントを実行した後、クラスターのクエリパフォーマンスが一時的に低下する可能性があります（コンパクションが完了するまで）。低下の程度は、指定する条件の数に応じて異なります。条件の数が多いほど、低下の程度が高くなります。

### 例

#### テーブルを作成してデータを挿入

次の例では、パーティション化された重複キーテーブルを作成します。

```SQL
CREATE TABLE `my_table` (
    `date` date NOT NULL,
    `k1` int(11) NOT NULL COMMENT "",
    `k2` varchar(65533) NULL DEFAULT "" COMMENT "")
DUPLICATE KEY(`date`)
PARTITION BY RANGE(`date`)
(
    PARTITION p1 VALUES [('2022-03-11'), ('2022-03-12')),
    PARTITION p2 VALUES [('2022-03-12'), ('2022-03-13'))
)
DISTRIBUTED BY HASH(`date`)
PROPERTIES
("replication_num" = "3");

INSERT INTO `my_table` VALUES
('2022-03-11', 3, 'abc'),
('2022-03-11', 2, 'acb'),
('2022-03-11', 4, 'abc'),
('2022-03-12', 2, 'bca'),
('2022-03-12', 4, 'cba'),
('2022-03-12', 5, 'cba');
```

#### データをクエリ

```plain
select * from my_table order by date;
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    3 | abc  |
| 2022-03-11 |    2 | acb  |
| 2022-03-11 |    4 | abc  |
| 2022-03-12 |    2 | bca  |
| 2022-03-12 |    4 | cba  |
| 2022-03-12 |    5 | cba  |
+------------+------+------+
```

#### データを削除

**指定されたパーティションからデータを削除**

`k1` の値が `3` の行を `p1` パーティションから削除します。

```plain
DELETE FROM my_table PARTITION p1
WHERE k1 = 3;

-- クエリ結果は、`k1` の値が `3` の行が削除されたことを示しています。

select * from my_table partition (p1);
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    2 | acb  |
| 2022-03-11 |    4 | abc  |
+------------+------+------+
```

**AND を使用して指定されたパーティションからデータを削除**

`k1` の値が `3` 以上で、`k2` の値が `"abc"` の行を `p1` パーティションから削除します。

```plain
DELETE FROM my_table PARTITION p1
WHERE k1 >= 3 AND k2 = "abc";

select * from my_table partition (p1);
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    2 | acb  |
+------------+------+------+
```

**すべてのパーティションからデータを削除**

`k2` の値が `"abc"` または `"cba"` の行をすべてのパーティションから削除します。

```plain
DELETE FROM my_table
WHERE  k2 in ("abc", "cba");

select * from my_table order by date;
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    2 | acb  |
| 2022-03-12 |    2 | bca  |
+------------+------+------+
```

## 主キーテーブル

v2.3から、主キーテーブルは完全な `DELETE...WHERE` セマンティクスをサポートし、主キー、任意のカラム、またはサブクエリに基づいてデータ行を削除できます。

### 構文

```SQL
[ WITH <with_query> [, ...] ]
DELETE FROM <table_name>
[ USING <from_item> [, ...] ]
[ WHERE <where_condition> ]
```

### パラメータ

|   **パラメータ**   | **必須** | **説明**                                              |
| :---------------: | :----------- | :----------------------------------------------------------- |
|   `with_query`    | いいえ           | DELETE ステートメントで名前で参照できる 1 つ以上の CTE。CTE は一時的な結果セットで、複雑なステートメントの可読性を向上させることができます。 |
|   `table_name`    | はい          | データを削除したいテーブル。                |
|    `from_item`    | いいえ           | データベース内の 1 つ以上の他のテーブル。これらのテーブルは、WHERE 句で指定された条件に基づいて操作対象のテーブルとジョインできます。ジョインクエリの結果セットに基づいて、StarRocks は操作対象のテーブルから一致する行を削除します。たとえば、USING 句が `USING t1 WHERE t0.pk = t1.pk;` の場合、StarRocks は DELETE ステートメントを実行する際に USING 句のテーブル式を `t0 JOIN t1 ON t0.pk=t1.pk;` に変換します。 |
| `where_condition` | はい          | 行を削除したい条件。この条件を満たす行のみが削除されます。このパラメータは、誤ってテーブル全体を削除するのを防ぐために必要です。テーブル全体を削除したい場合は、'WHERE true' を使用できます。 |

### 制限と使用上の注意

- 主キーテーブルは、指定されたパーティションからのデータ削除をサポートしていません。たとえば、`DELETE FROM <table_name> PARTITION <partition_id> WHERE <where_condition>` のような操作はできません。
- 次の比較演算子がサポートされています: `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, `NOT IN`。
- 次の論理演算子がサポートされています: `AND` および `OR`。
- DELETE ステートメントを使用して同時に DELETE 操作を実行したり、データロード時にデータを削除することはできません。これらの操作を行うと、トランザクションの原子性、一貫性、分離性、および耐久性 (ACID) が保証されない可能性があります。

### 例

#### テーブルを作成してデータを挿入

`score_board` という名前の主キーテーブルを作成します。

```sql
CREATE TABLE `score_board` (
  `id` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NULL DEFAULT "" COMMENT "",
  `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);

INSERT INTO score_board VALUES
(0, 'Jack', 21),
(1, 'Bob', 21),
(2, 'Stan', 21),
(3, 'Sam', 22);
```

#### データをクエリ

次のステートメントを実行して `score_board` テーブルにデータを挿入します。

```Plain
select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    0 | Jack |   21 |
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
4 rows in set (0.00 sec)
```

#### データを削除

**主キーでデータを削除**

DELETE ステートメントで主キーを指定できるため、StarRocks はテーブル全体をスキャンする必要がありません。

`score_board` テーブルから `id` の値が `0` の行を削除します。

```Plain
DELETE FROM score_board WHERE id = 0;

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
```

**条件でデータを削除**

例 1: `score_board` テーブルから `score` の値が `22` の行を削除します。

```Plain
DELETE FROM score_board WHERE score = 22;

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    0 | Jack |   21 |
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
+------+------+------+
```

例 2: `score_board` テーブルから `score` の値が `22` 未満の行を削除します。

```Plain
DELETE FROM score_board WHERE score < 22;

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    3 | Sam  |   22 |
+------+------+------+
```

例 3: `score_board` テーブルから `score` の値が `22` 未満で、`name` の値が `Bob` ではない行を削除します。

```Plain
DELETE FROM score_board WHERE score < 22 and name != "Bob";

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    1 | Bob  |   21 |
|    3 | Sam  |   22 |
+------+------+------+
2 rows in set (0.00 sec)
```

**サブクエリ結果でデータを削除**

`DELETE` ステートメントに 1 つ以上のサブクエリをネストし、サブクエリの結果を条件として使用できます。

データを削除する前に、次のステートメントを実行して `users` という名前の別のテーブルを作成します。

```SQL
CREATE TABLE `users` (
  `uid` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NOT NULL COMMENT "",
  `country` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`uid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`uid`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);
```

`users` テーブルにデータを挿入します。

```SQL
INSERT INTO users VALUES
(0, "Jack", "China"),
(2, "Stan", "USA"),
(1, "Bob", "China"),
(3, "Sam", "USA");
```

```plain
select * from users;
+------+------+---------+
| uid  | name | country |
+------+------+---------+
|    0 | Jack | China   |
|    1 | Bob  | China   |
|    2 | Stan | USA     |
|    3 | Sam  | USA     |
+------+------+---------+
4 rows in set (0.00 sec)
```

サブクエリをネストして `users` テーブルから `country` の値が `China` の行を見つけ、サブクエリから返された行と同じ `name` の値を持つ行を `score_board` テーブルから削除します。次のいずれかの方法を使用して目的を達成できます。

- 方法 1

```plain
DELETE FROM score_board
WHERE name IN (select name from users where country = "China");
    
select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
```

- 方法 2

```plain
DELETE FROM score_board
WHERE EXISTS (select name from users
              where score_board.name = users.name and country = "China");
    
select * from score_board;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|    2 | Stan |    21 |
|    3 | Sam  |    22 |
+------+------+-------+
2 rows in set (0.00 sec)
```

**マルチテーブルジョインまたは CTE を使用してデータを削除**

プロデューサー "foo" によって制作されたすべての映画を削除するには、次のステートメントを実行できます。

```SQL
DELETE FROM films USING producers
WHERE producer_id = producers.id
    AND producers.name = 'foo';
```

上記のステートメントを CTE を使用して書き換えることで、可読性を向上させることができます。

```SQL
WITH foo_producers as (
    SELECT * from producers
    where producers.name = 'foo'
)
DELETE FROM films USING foo_producers
WHERE producer_id = foo_producers.id;
```

## 参考文献

[SHOW DELETE](SHOW_DELETE.md): 重複キーテーブル、集計テーブル、およびユニークキーテーブルで正常に実行された過去の DELETE 操作をクエリします。