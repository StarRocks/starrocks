# from_base64

## 功能

解码 base64 编码过的参数`x`。

## 语法

```Haskell
from_base64(x);
```

## 参数说明

`x`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select from_base64("starrocks");
+--------------------------+
| from_base64('starrocks') |
+--------------------------+
| ²֫®$                       |
+--------------------------+
1 row in set (0.00 sec)
```
