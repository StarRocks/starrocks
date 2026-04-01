---
displayed_sidebar: docs
sidebar_label: "UNION"
---

# UNION

将多个查询的结果组合在一起。

## 语法

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

## 参数

- `DISTINCT` (默认): 仅返回唯一行。UNION 等同于 UNION DISTINCT。
- `ALL`: 合并所有行，包括重复行。由于去重操作会消耗大量内存，因此使用 UNION ALL 的查询速度更快，内存消耗更少。为了获得更好的性能，请使用 UNION ALL。

> **注意**
>
> 每个查询语句必须返回相同数量的列，并且这些列必须具有兼容的数据类型。

## 示例

创建表 `select1` 和 `select2`。

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

示例 1：返回两个表中的所有 ID，包括重复项。

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

示例 2：返回两个表中所有不重复的 ID。以下两个语句是等效的。

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

示例 3：返回两个表中所有唯一 ID 中的前三个 ID。以下两个语句是等效的。

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
