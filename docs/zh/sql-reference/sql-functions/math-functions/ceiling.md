# ceiling

## 功能

返回大于或等于 `x` 的最小整数。

## 语法

```Haskell
CEILING(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 BIGINT。

## 示例

```Plain Text
mysql> select ceiling(3.14);
+---------------+
| ceiling(3.14) |
+---------------+
|             4 |
+---------------+
1 row in set (0.00 sec)
```
