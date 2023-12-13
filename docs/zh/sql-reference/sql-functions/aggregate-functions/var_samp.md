---
displayed_sidebar: "Chinese"
---


# var_samp, variance_samp

## 功能

返回 expr 表达式的样本方差。

## 语法

```Haskell
VAR_SAMP(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
MySQL > select var_samp(scan_rows)
from log_statis
group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
```
