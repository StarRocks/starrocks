---
displayed_sidebar: docs
sidebar_label: "UNION"
---

# UNION

複数のクエリの結果を結合します。

## 構文

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

## パラメータ

- `DISTINCT` (デフォルト): 一意の行のみを返します。UNION は UNION DISTINCT と同等です。
- `ALL`: 重複を含むすべての行を結合します。重複排除はメモリを大量に消費するため、UNION ALL を使用したクエリの方が高速で、メモリ消費量も少なくなります。パフォーマンスを向上させるには、UNION ALL を使用してください。

> **NOTE**
>
> 各クエリステートメントは、同じ数の列を返し、列は互換性のあるデータ型を持っている必要があります。

## 例

テーブル `select1` と `select2` を作成します。

```SQL
CREATE TABLE select1(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select1 VALUES
    (1,2),
    (1,2),
    (2,3),
    (5,6),
    (5,6);

CREATE TABLE select2(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select2 VALUES
    (2,3),
    (3,4),
    (5,6),
    (7,8);
```

例1: 重複を含め、2つのテーブル内のすべてのIDを返します。

```Plaintext
mysql> (select id from select1) union all (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    1 |
|    2 |
|    2 |
|    3 |
|    5 |
|    5 |
|    5 |
|    7 |
+------+
11 rows in set (0.02 sec)
```

例 2：2 つのテーブルにある一意の ID をすべて返します。次の 2 つのステートメントは同等です。

```Plaintext
mysql> (select id from select1) union (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
6 rows in set (0.01 sec)

mysql> (select id from select1) union distinct (select id from select2) order by id;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
5 rows in set (0.02 sec)
```

例3：2つのテーブルにあるすべてのユニークなIDのうち、最初の3つのIDを返します。以下の2つのステートメントは同等です。

```SQL
mysql> (select id from select1) union distinct (select id from select2)
order by id
limit 3;
++------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
4 rows in set (0.11 sec)

mysql> select * from (select id from select1 union distinct select id from select2) as t1
order by id
limit 3;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
3 rows in set (0.01 sec)
```
