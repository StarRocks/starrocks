# curdate

## description

### Syntax

```Haskell
DATE CURDATE()
```

Obtain the current date and return in Date type.

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

## keyword

CURDATE
