---
displayed_sidebar: docs
---

# divide

## 功能

除法函数，返回 `x` 除以 `y` 的结果，如果 `y` 为 0，则返回null。

## 语法

```Haskell
divide(x, y);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE、FLOAT、LARGEINT、BIGINT、INT、SMALLINT、TINYINT、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

`y`: 支持的数据类型为 DOUBLE、FLOAT、LARGEINT、BIGINT、INT、SMALLINT、TINYINT、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select divide(3, 2);
+--------------+
| divide(3, 2) |
+--------------+
|          1.5 |
+--------------+
1 row in set (0.00 sec)
```
