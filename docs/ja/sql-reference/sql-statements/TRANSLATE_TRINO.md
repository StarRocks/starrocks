---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "From v3.3.9, StarRocks supports translating Trino SQL statements into StarRocks SQL."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# トリノ SQL を変換

<Beta />

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