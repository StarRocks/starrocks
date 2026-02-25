---
displayed_sidebar: docs
---

# max

expr 式の最大値を返します。

## Syntax

```Haskell
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