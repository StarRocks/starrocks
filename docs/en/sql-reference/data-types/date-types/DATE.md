---
displayed_sidebar: docs
---

# DATE

## Description

DATE type. The value range is ['0000-01-01', '9999-12-31'] and the default format is `YYYY-MM-DD`.

## Examples

Example 1: Specify a column as the DATE type when you create a table.

```SQL
CREATE TABLE dateDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    make_time DATE NOT NULL COMMENT "YYYY-MM-DD"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk)
```

Example 2: Convert a DATETIME value into a DATE value.

```sql
mysql> SELECT DATE('2003-12-31 01:02:03');
-> '2003-12-31'
```

For more information, see the [date](../../sql-functions/date-time-functions/date.md) function.
