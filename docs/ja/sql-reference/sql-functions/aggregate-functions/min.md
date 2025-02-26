---
displayed_sidebar: docs
---

# min

expr 式の最小値を返します。

## 構文

```Haskell
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