# regexp_replace

## description

### Syntax

```Haskell
VARCHAR regexp_replace(VARCHAR str, VARCHAR pattern, VARCHAR repl)
```

This function uses repl to replace a sequence of characters in str that matches a regular expression pattern.

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
