# STDDEV_SAMP

## 功能

返回 `expr` 表达式的样本标准差。从 2.5.10 版本开始，该函数也可以用作窗口函数。

## 语法

```Haskell
STDDEV_SAMP(expr)
```

## 参数说明

`exPr`: 被选取的表达式。当表达式为表中一列时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL。

## 返回值说明

返回值为 DOUBLE 类型。

## 示例

```plaintext
MySQL > select stddev_samp(scan_rows)
from log_statis
group by datetime;
+--------------------------+
| stddev_samp(`scan_rows`) |
+--------------------------+
|        2.372044195280762 |
+--------------------------+
```
