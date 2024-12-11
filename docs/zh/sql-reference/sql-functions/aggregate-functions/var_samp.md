---
displayed_sidebar: docs
---


# var_samp, variance_samp

<<<<<<< HEAD
## 功能

返回 expr 表达式的样本方差。
=======


返回 `expr` 表达式的样本方差。从 2.5.10 版本开始，该函数也可以用作窗口函数。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## 语法

```Haskell
VAR_SAMP(expr)
```

## 参数说明

<<<<<<< HEAD
`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
=======
`expr`: 被选取的表达式。当表达式为表中的一列时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL。

## 返回值说明

返回值为 DOUBLE 类型。

## 示例

```plaintext
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
MySQL > select var_samp(scan_rows)
from log_statis
group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
```
