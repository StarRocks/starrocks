---
displayed_sidebar: "English"
---

# current_timestamp

## Description

Obtains the current date and returns a value if the DATETIME type.

Since v3.1, the result is accurate to the microsecond.

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

-- The result is accurate to the microsecond since v3.1.
MySQL > select current_timestamp();
+----------------------------+
| current_timestamp()        |
+----------------------------+
| 2023-11-18 12:58:05.375000 |
+----------------------------+
```

## keyword

CURRENT_TIMESTAMP,CURRENT,TIMESTAMP
