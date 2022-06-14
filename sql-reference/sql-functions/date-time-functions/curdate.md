# curdate

## description

### Syntax

```Haskell
DATE CURDATE()
```

获取当前的日期，以DATE类型返回

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
