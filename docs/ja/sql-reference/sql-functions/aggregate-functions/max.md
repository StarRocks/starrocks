---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "expr 式の最大値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# max

expr 式の最大値を返します。

## Syntax

```plaintext
MAX(expr)
```

## 例

```plain text
MySQL > select max(scan_rows)
from log_statis
group by datetime;
+------------------+
| max(`scan_rows`) |
+------------------+
|          4671587 |
+------------------+
```

## キーワード

MAX