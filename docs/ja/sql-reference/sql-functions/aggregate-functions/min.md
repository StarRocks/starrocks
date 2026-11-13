---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "expr 式の最小値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# min

expr 式の最小値を返します。

## 構文

```plaintext
MIN(expr)
```

## 例

```plain text
MySQL > select min(scan_rows)
from log_statis
group by datetime;
+------------------+
| min(`scan_rows`) |
+------------------+
|                0 |
+------------------+
```

## キーワード

MIN