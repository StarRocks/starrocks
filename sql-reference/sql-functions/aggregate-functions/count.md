
# COUNT

## 功能

用于返回满足要求的行的数目。

## 语法

```Haskell
COUNT([DISTINCT] expr)
```

## 参数说明

`epxr`: 被选取的表达式, 可字段 DISTINCT 参数。

## 返回值说明

返回值为数值类型。

## 示例

```plain text

MySQL > select count(*)
from log_statis
group by datetime;
+----------+
| count(*) |
+----------+
| 28515903 |
+----------+


MySQL > select count(datetime)
from log_statis
group by datetime;
+-------------------+
| count(`datetime`) |
+-------------------+
|         28521682  |
+-------------------+

MySQL > select count(distinct datetime)
from log_statis
group by datetime;
+----------------------------+
| count(DISTINCT `datetime`) |
+----------------------------+
|                    71045   |
+----------------------------+
```
