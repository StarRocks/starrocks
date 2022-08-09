# curdate

## 功能

获取当前的日期，以DATE类型返回。

## 语法

```Haskell
DATE CURDATE()
```

## 示例

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
