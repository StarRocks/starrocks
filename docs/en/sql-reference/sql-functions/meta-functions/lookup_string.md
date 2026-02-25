---
displayed_sidebar: docs
---

# lookup_string

`lookup_string(table_name, lookup_key, return_column)`

This function looks up a value from a primary key table and evaluates it in the optimizer.

## Arguments

`table_name`: The name of the table to lookup. Must be a primary-key table (VARCHAR).
`lookup_key`: The key to lookup. Must be a string type (VARCHAR).
`return_column`: The name of the column to return (VARCHAR).

## Return Value

Returns a VARCHAR string containing the looked-up value. Returns `NULL` if not found.

## Examples

Example 1: Return `t2` table's `event_day` column value of `t2` table with its primay key value equals to `1`;
```
mysql> select lookup_string('t2', '1', 'event_day');
+---------------------------------------+
| lookup_string('t2', '1', 'event_day') |
+---------------------------------------+
| 2020-01-14                            |
+---------------------------------------+
1 row in set (0.02 sec)

```