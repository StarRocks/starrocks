# regexp

## 功能

对参数 `expr1` 进行正则匹配，匹配成功返回 1，否则返回 0。

## 语法

```Haskell
regexp(expr1,expr2);
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`pattern`: 支持的数据类型为 VARCHAR，正则表达式。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
mysql> select `regexp`("abc123","abc*");
+--------------------------+
| regexp('abc123', 'abc*') |
+--------------------------+
|                        1 |
+--------------------------+
1 row in set (0.06 sec)
```
