---
displayed_sidebar: docs
---

# current_timestamp

## Description

Obtains the current date and returns a value if the DATETIME type.

This function is a synonym of the [now()](./now.md) function.

## Syntax

```Haskell
DATETIME CURRENT_TIMESTAMP()
```

## Examples

Example 1: Return the current time.

```Plain Text
MySQL > select current_timestamp();
+---------------------+
| current_timestamp() |
+---------------------+
| 2019-05-27 15:59:33 |
+---------------------+
```

Example 2: When you create a table, you can use this function for a column so that current time is the default value of the column.

```SQL
CREATE TABLE IF NOT EXISTS sr_member (
    sr_id            INT,
    name             STRING,
    city_code        INT,
    reg_date         DATETIME DEFAULT current_timestamp,
    verified         BOOLEAN
);
```

## keyword

CURRENT_TIMESTAMP,CURRENT,TIMESTAMP
