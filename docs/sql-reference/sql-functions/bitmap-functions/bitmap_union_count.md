# bitmap_union_count

## Description

Returns the union of a set of bitmap values and returns the cardinality of the union.

## Syntax

```Plain
BIGINT bitmap_union_count(BITMAP value)
```

### Parameters

`value`: a set of bitmap values. The supported data type is BITMAP.

## Return value

Returns a value of the BIGINT type.

## Examples

Calculate the page views (PVs) of a web page. Assume that `user_id` is of the INT type. The latter two queries are equivalent.

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
