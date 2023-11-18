---
displayed_sidebar: "Chinese"
---


# stddev, stddev_pop, std

## 功能

返回 `expr` 表达式的总体标准差。从 2.5.10 版本开始，该函数也可以用作窗口函数。

### 语法

```Haskell
STDDEV(expr)
```

## 参数说明

`expr`: 被选取的表达式。当表达式为表中一列时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL。

## 返回值说明

返回值为 DOUBLE 类型。

## 示例

```plaintext
mysql> SELECT stddev(lo_quantity), stddev_pop(lo_quantity) from lineorder;
+---------------------+-------------------------+
| stddev(lo_quantity) | stddev_pop(lo_quantity) |
+---------------------+-------------------------+
|   14.43100708360797 |       14.43100708360797 |
+---------------------+-------------------------+
```
