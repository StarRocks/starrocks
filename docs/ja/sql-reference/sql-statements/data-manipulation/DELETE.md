---
displayed_sidebar: docs
---

# DELETE

指定された条件に基づいてテーブルからデータ行を削除します。テーブルはパーティション化されているものとされていないもののどちらでもかまいません。

重複キーテーブル、集計テーブル、およびユニークキーテーブルでは、指定されたパーティションからデータを削除できます。v2.3から、主キーテーブルは完全な `DELETE...WHERE` セマンティクスをサポートしており、主キー、任意のカラム、またはサブクエリの結果に基づいてデータ行を削除できます。

## 使用上の注意

- DELETE を実行したいテーブルとデータベースに対する権限を持っている必要があります。
- 頻繁な DELETE 操作は推奨されません。必要な場合は、ピーク時間外に実行してください。
- DELETE 操作はテーブル内のデータのみを削除します。テーブル自体は残ります。テーブルを削除するには、[DROP TABLE](../data-definition/DROP_TABLE.md) を実行してください。
- 誤操作でテーブル全体のデータを削除しないように、DELETE 文には WHERE 句を指定する必要があります。
- 削除された行はすぐにはクリーンアップされません。それらは「削除済み」とマークされ、セグメントに一時的に保存されます。物理的には、データバージョンのマージ（コンパクション）が完了した後にのみ行が削除されます。
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
| `op`             | はい      | DELETE 条件で使用される演算子。サポートされている演算子は `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, `NOT IN` です。 |

### 制限と使用上の注意

- 重複キーテーブルでは、**任意のカラム** を DELETE 条件として使用できます。集計テーブルとユニークキーテーブルでは、**キー列** のみを DELETE 条件として使用できます。

- 指定する条件は AND 関係でなければなりません。OR 関係で条件を指定したい場合は、別々の DELETE 文で条件を指定する必要があります。

- 重複キーテーブル、集計テーブル、およびユニークキーテーブルでは、DELETE 文でサブクエリの結果を条件として使用することはできません。

### 影響

DELETE 文を実行した後、クラスターのクエリパフォーマンスが一時的に低下する可能性があります（コンパクションが完了するまで）。指定する条件の数に応じて、低下の程度は異なります。条件の数が多いほど、低下の程度が高くなります。

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
DISTRIBUTED BY HASH(`date`) BUCKETS 1
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

v2.3から、主キーテーブルは完全な `DELETE...WHERE` セマンティクスをサポートしており、主キー、任意のカラム、またはサブクエリに基づいてデータ行を削除できます。

### 構文

```SQL
DELETE FROM <table_name> WHERE <condition>;
```

### パラメータ

| **パラメータ**         | **必須** | **説明**                                                     |
| :--------------- | :------- | :----------------------------------------------------------- |
| `table_name`     | はい      | データを削除したいテーブル。   |
| `condition`      | はい      | データを削除するための条件。1つ以上の条件を指定できます。このパラメータは、誤操作でテーブル全体を削除しないようにするために必須です。 |

### 制限と使用上の注意

- 主キーテーブルは、指定されたパーティションからのデータ削除をサポートしていません。例えば、`DELETE FROM <table_name> PARTITION <partition_id> WHERE <where_condition>`。
- サポートされている比較演算子は次のとおりです：`=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, `NOT IN`。
- サポートされている論理演算子は次のとおりです：`AND` と `OR`。
- DELETE 文を使用して同時に DELETE 操作を実行したり、データロード時にデータを削除することはできません。このような操作を行うと、トランザクションの原子性、一貫性、分離性、および耐久性（ACID）が保証されない可能性があります。

### 例

#### テーブルを作成してデータを挿入

`score_board` という名前の主キーテーブルを作成します：

```sql
CREATE TABLE `score_board` (
  `id` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NULL DEFAULT "" COMMENT "",
  `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
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

次の文を実行して `score_board` テーブルにデータを挿入します：

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

DELETE 文で主キーを指定することができるため、StarRocks はテーブル全体をスキャンする必要がありません。

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

例 3: `score_board` テーブルから `score` の値が `22` 未満で、`name` の値が `Bob` でない行を削除します。

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

**サブクエリの結果でデータを削除**

`DELETE` 文に1つ以上のサブクエリをネストし、サブクエリの結果を条件として使用することができます。

データを削除する前に、`users` という別のテーブルを作成するために次の文を実行します：

```SQL
CREATE TABLE `users` (
  `uid` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NOT NULL COMMENT "",
  `country` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`uid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`uid`) BUCKETS 1
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);
```

`users` テーブルにデータを挿入します：

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

サブクエリをネストして、`users` テーブルから `country` の値が `China` の行を見つけ、そのサブクエリから返された行と同じ `name` の値を持つ行を `score_board` テーブルから削除します。次の方法のいずれかを使用して目的を達成できます：

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

## 参考文献

[SHOW DELETE](./SHOW_DELETE.md): 重複キーテーブル、集計テーブル、およびユニークキーテーブルで正常に実行された過去の DELETE 操作をクエリします。