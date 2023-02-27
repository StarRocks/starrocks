# bitmap_union_count

## Description

Returns the union of a set of bitmap values and returns the cardinality of the union. This function is supported from v2.3.

## Syntax

```Haskell
BIGINT bitmap_union_count(BITMAP value)
```

### Parameters

`value`: a set of bitmap values. The supported data type is BITMAP.

## Return value

Returns a value of the BIGINT type.

## Examples

Calculate the unique views (UVs) of a web page. If `user_id` is of the INT type, the latter two queries are equivalent.

```Plaintext
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
