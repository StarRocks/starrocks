# regexp

## 功能

判断目标字符串 `expr` 是否匹配给定的正则表达式 `pattern`。匹配成功返回 1，否则返回 0。如果任何一个输入参数为 NULL，则返回 NULL。

## 语法

```Haskell
BOOLEAN regexp(VARCHAR expr, VARCHAR pattern);
```

## 参数说明

`expr`: 目标字符串，支持的数据类型为 VARCHAR。

`pattern`: 正则表达式，即字符串需匹配的模式，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
mysql> select regexp("abc123","abc*");
+--------------------------+
| regexp('abc123', 'abc*') |
+--------------------------+
|                        1 |
+--------------------------+
1 row in set (0.06 sec)
```

## Keywords

regexp, regular
