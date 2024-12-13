---
displayed_sidebar: docs
---

# max

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

返回 expr 表达式的最大值。

## 语法

```Haskell
MAX(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

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
