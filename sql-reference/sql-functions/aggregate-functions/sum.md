
# SUM

## 功能

用于返回选中字段所有值的和

## 语法

```Haskell
SUM(expr)
```

## 参数说明

`epxr`: 被选取的表达式

## 返回值说明

返回值的数据类型为 INT

## 示例

```plain text
MySQL > select sum(scan_rows)
from log_statis
group by datetime;
+------------------+
| sum(`scan_rows`) |
+------------------+
|       8217360135 |
+------------------+
```
