# curdate

## description

### Syntax

```Haskell
DATE CURDATE()
```

获取当前的日期，以DATE类型返回

## Examples

```Plain Text
SELECT CURDATE();
+------------+
| CURDATE()  |
+------------+
| 2019-12-20 |
+------------+

SELECT CURDATE() + 0;
+---------------+
| CURDATE() + 0 |
+---------------+
|      20191220 |
+---------------+
```

## keyword

CURDATE
