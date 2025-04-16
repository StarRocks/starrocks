---
displayed_sidebar: docs
---

# current_timestamp



Obtains the current date and returns a value if the DATETIME type.

This function is a synonym of the [now()](./now.md) function.

## Syntax

```Haskell
DATETIME CURRENT_TIMESTAMP()
DATETIME CURRENT_TIMESTAMP(INT p)
```

## Parameters

`p`: optional, the specified precision, that is, the number of digits to retain after seconds. It must be an INT value within the range of [1,6]. `select current_timestamp(0)` is equivalent to `select current_timestamp()`.

## Return value

- If `p` is not specified, this function returns a DATETIME value accurate to the second.
- If `p` is specified, this function returns a date and time value of the specified precision.

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
