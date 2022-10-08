# curdate

## 功能

获取当前的日期，以DATE类型返回。

## 语法

```Haskell
DATE CURDATE()
```

## 示例

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
