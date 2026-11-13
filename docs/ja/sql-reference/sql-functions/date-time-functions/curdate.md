---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DATE または DATETIME の値をあるタイムゾーンから別のタイムゾーンに変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# curdate,current_date

現在の日付を取得し、DATE 型の値を返します。

## Syntax

```plaintext
DATE CURDATE()
```

## Examples

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
