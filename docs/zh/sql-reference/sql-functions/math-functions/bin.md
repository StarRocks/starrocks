# bin

## 功能

将参数`x`转成二进制。

## 语法

```Haskell
BIN(x);
```

## 参数说明

`x`: 支持的数据类型为BIGINT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql>select bin(3);
+--------+
| bin(3) |
+--------+
| 11     |
+--------+
1 row in set (0.02 sec)
```
