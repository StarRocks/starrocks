---
displayed_sidebar: docs
---

# lower_utf8



将参数中的UTF8字符串转换成小写。如果输入不是合法的UTF8编码字符串，可能会产生未定义行为。

## 语法

```Haskell
lower_utf8(str)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 语法说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select lower_utf8('Hallo, München');
+-------------------------------+
| lower_utf8('Hallo, München')  |
+-------------------------------+
| hallo, münchen                |
+-------------------------------+
```
