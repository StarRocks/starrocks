---
displayed_sidebar: "English"
---

# date

## Description

Extracts the date part of a date or datetime expression.

## Syntax

```Haskell
DATE date(DATETIME|DATE expr)
```

## Parameters

`expr`: the date or datetime expression.

## Return value

Returns a value of the DATE type. NULL is returned if the input is NULL or invalid.

## Examples

Example 1: Extract the date part of a datetime value.

```plaintext
SELECT DATE("2017-12-31 11:20:59");
+-----------------------------+
| date('2017-12-31 11:20:59') |
+-----------------------------+
| 2017-12-31                  |
+-----------------------------+
1 row in set (0.05 sec)
```

Example 2: Extract the date part of a date value.

```plaintext
SELECT DATE('2017-12-31');
+--------------------+
| date('2017-12-31') |
+--------------------+
| 2017-12-31         |
+--------------------+
1 row in set (0.08 sec)
```

Example 3: Extract the date part of the current timestamp.

```plaintext
SELECT DATE(current_timestamp());
+---------------------------+
| date(current_timestamp()) |
+---------------------------+
| 2022-11-08                |
+---------------------------+
1 row in set (0.05 sec)
```
