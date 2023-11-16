# AVG

## 功能

用于返回选中字段的平均值。

## 语法

```Haskell
AVG([DISTINCT] expr)
```

## 参数说明

`epxr`: 被选取的表达式，可选字段 DISTINCT 参数。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
MySQL > SELECT datetime, AVG(cost_time)
FROM log_statis
group by datetime;
+---------------------+--------------------+
| datetime            | avg(`cost_time`)   |
+---------------------+--------------------+
| 2019-07-03 21:01:20 | 25.827794561933533 |
+---------------------+--------------------+

MySQL > SELECT datetime, AVG(distinct cost_time)
FROM log_statis
group by datetime;
+---------------------+---------------------------+
| datetime            | avg(DISTINCT `cost_time`) |
+---------------------+---------------------------+
| 2019-07-04 02:23:24 |        20.666666666666668 |
+---------------------+---------------------------+

```
