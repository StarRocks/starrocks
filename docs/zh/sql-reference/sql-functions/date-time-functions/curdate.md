# curdate,current_date

## 功能

获取当前的日期，以 DATE 类型返回。

## 语法

```Haskell
DATE CURDATE()
```

## 示例

```Plain Text
SELECT CURDATE();
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

SELECT CURDATE() + 0;
+-----------------+
| (curdate()) + 0 |
+-----------------+
|        20221220 |
+-----------------+
```
