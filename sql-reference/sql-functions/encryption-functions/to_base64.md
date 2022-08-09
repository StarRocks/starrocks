# to_base64

## 功能

将参数 `x` 进行 Base64 编码。

## 语法

```Haskell
to_base64(x);
```

## 参数说明

`x`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select to_base64("starrocks");
+------------------------+
| to_base64('starrocks') |
+------------------------+
| c3RhcnJvY2tz           |
+------------------------+
1 row in set (0.00 sec)
```
