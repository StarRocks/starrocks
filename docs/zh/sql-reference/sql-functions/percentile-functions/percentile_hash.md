# percentile_hash

## 功能

将 `double` 类型数值构造成 `percentile` 类型数值。

## 语法

```Haskell
PERCENTILE_HASH(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 PERCENTILE。

## 示例

```Plain Text
mysql> select percentile_approx_raw(percentile_hash(234.234), 0.99);
+-------------------------------------------------------+
| percentile_approx_raw(percentile_hash(234.234), 0.99) |
+-------------------------------------------------------+
|                                    234.23399353027344 |
+-------------------------------------------------------+
1 row in set (0.00 sec)
```
