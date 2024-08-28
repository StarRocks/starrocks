---
displayed_sidebar: docs
---

# curdate,current_date

## 功能

获取当前的日期，以 DATE 类型返回。

## 语法

```Haskell
DATE CURDATE()
```

## 示例

```Plain Text
mysql> SELECT CURDATE();
+------------+
| curdate()  |
+------------+
| 2024-03-27 |
+------------+

mysql> SELECT CURRENT_DATE();
+----------------+
| current_date() |
+----------------+
| 2024-03-27     |
+----------------+

mysql> SELECT CURDATE() + 0;
+-----------------+
| (curdate()) + 0 |
+-----------------+
|        20240327 |
+-----------------+

mysql> SELECT CURDATE() +2;
+-----------------+
| (curdate()) + 2 |
+-----------------+
|        20240329 |
+-----------------+

mysql> SELECT CURDATE() - INTERVAL 5 DAY;
+----------------------------+
| curdate() - INTERVAL 5 DAY |
+----------------------------+
| 2024-03-22 00:00:00        |
+----------------------------+

mysql> SELECT QUARTER(CURDATE());
+--------------------+
| quarter(curdate()) |
+--------------------+
|                  1 |
+--------------------+
```
