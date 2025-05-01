---
displayed_sidebar: docs
---

# split_part

## Description

この関数は、指定された文字列を区切り文字に従って分割し、指定された部分を返します。（最初から数え始めます）

## Syntax

```Haskell
VARCHAR split_part(VARCHAR content, VARCHAR delimiter, INT field)
```

## Examples

```Plain Text
MySQL > select split_part("hello world", " ", 1);
+----------------------------------+
|split_part('hello world', ' ', 1) |
+----------------------------------+
| hello                            |
+----------------------------------+

MySQL > select split_part("hello world", " ", 2);
+-----------------------------------+
| split_part('hello world', ' ', 2) |
+-----------------------------------+
| world                             |
+-----------------------------------+

MySQL > select split_part("abca", "a", 1);
+----------------------------+
| split_part('abca', 'a', 1) |
+----------------------------+
|                            |
+----------------------------+
```

## keyword

SPLIT_PART, SPLIT, PART