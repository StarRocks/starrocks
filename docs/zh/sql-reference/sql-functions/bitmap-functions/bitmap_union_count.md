---
displayed_sidebar: "Chinese"
---

# bitmap_union_count

## 功能

计算输入的一组 bitmap 值的并集，并返回并集的基数。该函数从 2.3 版本开始支持。

## 语法

```Haskell
BITMAP_UNION_COUNT(value)
```

## 参数说明

- `value` ：输入的一组 bitmap 值，支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BIGINT。

## 示例

使用该函数计算网页的 UV 数据。假设 `user_id` 字段类型为 INT，下面两个查询是等价的。

```sql
mysql> select * from test
+---------+---------+
| page_id | user_id |
+---------+---------+
|       1 |       1 |
|       1 |       2 |
|       2 |       1 |
+---------+---------+

mysql> select page_id,count(distinct user_id) from test group by page_id;
+---------+-------------------------+
| page_id | count(DISTINCT user_id) |
+---------+-------------------------+
|       1 |                       2 |
|       2 |                       1 |
+---------+-------------------------+

mysql> select page_id,bitmap_union_count(to_bitmap(user_id)) from test group by page_id;
+---------+----------------------------------------+
| page_id | bitmap_union_count(to_bitmap(user_id)) |
+---------+----------------------------------------+
|       1 |                                      2 |
|       2 |                                      1 |
+---------+----------------------------------------+

```
