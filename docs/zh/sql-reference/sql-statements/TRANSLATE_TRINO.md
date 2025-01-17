---
displayed_sidebar: docs
---

# 翻译 Trino SQL

自 v3.3.9 起，StarRocks 支持将 Trino SQL 语句翻译为 StarRocks SQL 语句。

## 语法

```SQL
TRANSLATE TRINO <SELECT_statement>
```

## 参数说明

`SELECT_statement`：需要翻译的 Trino SQL 语句。

## 返回

返回 StarRocks SQL 语句。

## 示例

```Plain
mysql> TRANSLATE TRINO SELECT id, name, category FROM products WHERE name = 'Dell XPS 13'  AND category = "Electronics"  AND `price` > 500;
+---------------------------------------------------------------------------------------------------------------------------------------+
| Translated SQL                                                                                                                        |
+---------------------------------------------------------------------------------------------------------------------------------------+
| SELECT `id`, `name`, `category`
FROM `products`
WHERE ((`name` = 'Dell XPS 13') AND (`category` = 'Electronics')) AND (`price` > 500) |
+---------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.30 sec)
```
