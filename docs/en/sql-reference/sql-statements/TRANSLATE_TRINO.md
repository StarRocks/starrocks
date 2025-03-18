---
displayed_sidebar: docs
---

# Translate Trino SQL

From v3.3.9, StarRocks supports translating Trino SQL statements into StarRocks SQL statements.

## Syntax

```SQL
TRANSLATE TRINO <SELECT_statement>
```

## Parameters

`SELECT_statement`: The Trino SQL statement you want to translate.

## Return

Returns the StarRocks SQL statement.

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
