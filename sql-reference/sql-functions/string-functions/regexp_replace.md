# regexp_replace

## description

### Syntax

```Haskell
VARCHAR regexp_replace(VARCHAR str, VARCHAR pattern, VARCHAR repl)
```

对字符串 str 进行正则匹配, 将命中 pattern 的部分使用 repl 来进行替换

## example

```Plain Text
MySQL > SELECT regexp_replace('a b c', " ", "-");
+-----------------------------------+
| regexp_replace('a b c', ' ', '-') |
+-----------------------------------+
| a-b-c                             |
+-----------------------------------+

MySQL > SELECT regexp_replace('a b c','(b)','<\\1>');
+----------------------------------------+
| regexp_replace('a b c', '(b)', '<\1>') |
+----------------------------------------+
| a <b> c                                |
+----------------------------------------+
```

## keyword

REGEXP_REPLACE,REGEXP,REPLACE
