# lcase

## 功能

该函数与 `lower` 一致，将字符串转换为小写形式。

## 语法

```Haskell
lcase(str)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```
