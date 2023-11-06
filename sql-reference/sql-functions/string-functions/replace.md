# replace

## 功能

将一个字符串 (`str`) 中符合指定模式的字符 (`pattern`) 全部替换成其他字符 (`repl`)。注意替换时会区分大小写。

该函数从 3.0 版本开始支持。

> **注意**
>
> 在 3.0 版本之前，该函数通过 [regexp_replace](../like_predicate-functions/regexp_replace.md) 来实现。

## 语法

```Haskell
VARCHAR replace(VARCHAR str, VARCHAR pattern, VARCHAR repl)
```

## 参数说明

- `str`: 原始字符串。

- `pattern`: 用于匹配字符的表达式。不能是正则表达式。

- `repl`: 用于替换 `pattern` 中指定的字符。

## 返回值说明

返回一个字符串。

如果任意一个输入参数为 NULL，则返回 NULL。

如果未找到符合条件的字符，则返回原始的字符串。

## 示例

```Plain Text
-- 将字符串 'a.b.c' 里的 '.' 都替换成 '+'。

MySQL > SELECT replace('a.b.c', '.', '+');
+----------------------------+
| replace('a.b.c', '.', '+') |
+----------------------------+
| a+b+c                      |
+----------------------------+

-- 未找到符合条件的字符，返回原始字符串。

MySQL > SELECT replace('a b c', '', '*');
+----------------------------+
| replace('a b c', '', '*') |
+----------------------------+
| a b c                      |
+----------------------------+

-- 将 'like' 替换为空字符串。

MySQL > SELECT replace('We like StarRocks', 'like', '');
+------------------------------------------+
| replace('We like StarRocks', 'like', '') |
+------------------------------------------+
| We  StarRocks                            |
+------------------------------------------+

-- 未找到符合条件的字符，返回原始字符串。

MySQL > SELECT replace('He is awesome', 'handsome', 'smart');
+-----------------------------------------------+
| replace('He is awesome', 'handsome', 'smart') |
+-----------------------------------------------+
| He is awesome                                 |
+-----------------------------------------------+
```

## keywords

REPLACE, replace
