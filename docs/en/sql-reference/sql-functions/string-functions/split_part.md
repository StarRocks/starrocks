# split_part

## Description

This function splits a given string according to the separators and returns the requested part. (start counting from the beginning)

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

MySQL > select split_part("hello world", " ", -1);
+----------------------------------+
|split_part('hello world', ' ', -1) |
+----------------------------------+
| world                            |
+----------------------------------+

MySQL > select split_part("hello world", " ", -2);
+-----------------------------------+
| split_part('hello world', ' ', -2) |
+-----------------------------------+
| hello                             |
+-----------------------------------+

MySQL > select split_part("abca", "a", 1);
+----------------------------+
| split_part('abca', 'a', 1) |
+----------------------------+
|                            |
+----------------------------+

select split_part("abca", "a", -1);
+-----------------------------+
| split_part('abca', 'a', -1) |
+-----------------------------+
|                             |
+-----------------------------+

select split_part("abca", "a", -2);
+-----------------------------+
| split_part('abca', 'a', -2) |
+-----------------------------+
| bc                          |
+-----------------------------+
```

## keyword

SPLIT_PART,SPLIT,PART
