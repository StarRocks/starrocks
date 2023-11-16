# ltrim

## 功能

从字符串的左侧（开始部分）移除连续出现的空格或指定字符。从 2.5.0 版本开始，支持从字符串中移除指定的字符。

## 语法

```Haskell
ltrim(str[, characters])
```

## 参数说明

`str`: 必选，待裁剪的字符串，支持的数据类型为 VARCHAR。

`characters`: 可选，要移除的字符，支持的数据类型为 VARCHAR。如果不指定该参数，默认移除空格。如果该参数为空字符，返回报错。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

示例一：移除字符串左侧的连续空格。

```Plain Text
MySQL > SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```

示例二：移除字符串左侧的指定字符。

```Plain Text
MySQL > SELECT ltrim("xxabcdxx", "x");
+------------------------+
| ltrim('xxabcdxx', 'x') |
+------------------------+
| abcdxx                 |
+------------------------+
```

## 相关文档

- [trim](trim.md)
- [rtrim](rtrim.md)
