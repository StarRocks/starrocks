---
displayed_sidebar: docs
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Translate Trino SQL

<Beta />

从 v3.3.9 开始，StarRocks 支持将 Trino SQL 语句转换为 StarRocks SQL 语句。

## 语法

```SQL
TRANSLATE TRINO <SELECT_statement>
```

## 参数

`SELECT_statement`：您想要转换的 Trino SQL 语句。

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