# curdate,current_date

## Description

Obtains the current date and returns a value of the DATE type.

## Syntax

```Haskell
DATE CURDATE()
```

## Examples

```Plain Text
MySQL > SELECT CURDATE();
+------------+
| curdate()  |
+------------+
| 2022-12-20 |
+------------+

SELECT CURRENT_DATE();
+----------------+
| current_date() |
+----------------+
| 2022-12-20     |
+----------------+


MySQL > SELECT CURDATE() + 0;
+---------------+
| CURDATE() + 0 |
+---------------+
|      20191220 |
+---------------+
```
