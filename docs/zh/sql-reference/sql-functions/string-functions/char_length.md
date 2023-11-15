# char_length

## description

### Syntax

```Haskell
INT char_length(VARCHAR str)
```

返回字符串的长度，对于多字节字符，返回**字符**数, 目前仅支持utf8 编码。这个函数还有一个别名 `character_length`。

## example

```Plain Text
MySQL > select char_length("abc");
+--------------------+
| char_length('abc') |
+--------------------+
|                  3 |
+--------------------+

MySQL > select char_length("中国");
+----------------------+
| char_length('中国')  |
+----------------------+
|                    2 |
+----------------------+
```

## keyword

CHAR_LENGTH, CHARACTER_LENGTH
