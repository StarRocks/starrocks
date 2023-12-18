---
displayed_sidebar: "Chinese"
---

# bitnot

## 功能

返回参数 `x` 进行取反运算后的结果。

## 语法

```Haskell
BITNOT(x);
```

## 参数说明

`x`: 支持的数据类型为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

## 返回值说明

返回值的数据类型与 `x` 类型一致。

## 示例

```Plain Text
mysql> select bitnot(3);
+-----------+
| bitnot(3) |
+-----------+
|        -4 |
+-----------+
1 row in set (0.00 sec)
```
