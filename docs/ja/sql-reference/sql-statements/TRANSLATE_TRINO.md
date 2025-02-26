---
displayed_sidebar: docs
---

# Translate Trino SQL

v3.3.9 から、StarRocks は Trino SQL ステートメントを StarRocks SQL ステートメントに変換することをサポートしています。

## Syntax

```SQL
TRANSLATE TRINO <SELECT_statement>
```

## Parameters

`SELECT_statement`: 変換したい Trino SQL ステートメント。

## Return

StarRocks SQL ステートメントを返します。

## Examples

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