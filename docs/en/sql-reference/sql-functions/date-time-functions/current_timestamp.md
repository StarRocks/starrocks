---
displayed_sidebar: "English"
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

```Plain Text
MySQL > select current_timestamp();
+---------------------+
| current_timestamp() |
+---------------------+
| 2019-05-27 15:59:33 |
+---------------------+
```

```
mysql> CREATE TABLE IF NOT EXISTS sr_member (
    ->     sr_id            INT,
    ->     name             STRING,
    ->     city_code        INT,
    ->     reg_date         DATETIME DEFAULT current_timestamp,
    ->     verified         BOOLEAN
    -> );
Query OK, 0 rows affected (0.01 sec)
```

## keyword

CURRENT_TIMESTAMP,CURRENT,TIMESTAMP
