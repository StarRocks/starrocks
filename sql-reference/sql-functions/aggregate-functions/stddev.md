
# STDDEV, STDDEV_POP

## 功能

返回 expr 表达式的标准差。

### 语法

```Haskell
STDDEV(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
select stddev(scan_rows)
from log_statis
group by datetime;
+---------------------+
| stddev(`scan_rows`) |
+---------------------+
|  2.3736656687790934 |
+---------------------+

select stddev_pop(scan_rows)
from log_statis
group by datetime;
+-------------------------+
| stddev_pop(`scan_rows`) |
+-------------------------+
|      2.3722760595994914 |
+-------------------------+
```
