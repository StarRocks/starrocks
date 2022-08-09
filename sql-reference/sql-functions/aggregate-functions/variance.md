
# VARIANCE, VAR_POP, VARIANCE_POP

## 功能

返回 expr 表达式的方差。

## 语法

```Haskell
VARIANCE(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
MySQL > select variance(scan_rows)
from log_statis
group by datetime;
+-----------------------+
| variance(`scan_rows`) |
+-----------------------+
|    5.6183332881176211 |
+-----------------------+

MySQL > select var_pop(scan_rows)
from log_statis
group by datetime;
+----------------------+
| var_pop(`scan_rows`) |
+----------------------+
|   5.6230744719006163 |
+----------------------+
```
