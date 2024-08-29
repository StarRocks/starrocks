---
displayed_sidebar: docs
---

# min

## 功能

返回 expr 表达式的最小值。

## 语法

```Haskell
MIN(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

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
