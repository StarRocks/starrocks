# curdate

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
| CURDATE()  |
+------------+
| 2019-12-20 |
+------------+

MySQL > SELECT CURDATE() + 0;
+---------------+
| CURDATE() + 0 |
+---------------+
|      20191220 |
+---------------+
```
