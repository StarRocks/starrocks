# substring_index

## Description

This function splits a given string according to the separators and returns the requested part. (start counting from the beginning)

## Syntax

```Haskell
VARCHAR substring_index(VARCHAR content, VARCHAR delimiter, INT field)
```

## Examples

```Plain Text
MySQL > select substring_index("hello world", " ", 1);
+----------------------------------+
|split_part('hello world', ' ', 1) |
+----------------------------------+
| hello                            |
+----------------------------------+

MySQL > select substring_index("hello world", " ", 2);
+-----------------------------------+
| split_part('hello world', ' ', 2) |
+-----------------------------------+
| world                             |
+-----------------------------------+

MySQL > select substring_index("hello world", " ", -1);
+----------------------------------+
|split_part('hello world', ' ', -1) |
+----------------------------------+
| world                            |
+----------------------------------+

MySQL > select substring_index("hello world", " ", -2);
+-----------------------------------+
| split_part('hello world', ' ', -2) |
+-----------------------------------+
| hello                             |
+-----------------------------------+

MySQL > select substring_index("abca", "a", 1);
+----------------------------+
| split_part('abca', 'a', 1) |
+----------------------------+
|                            |
+----------------------------+

select substring_index("abca", "a", -1);
+-----------------------------+
| split_part('abca', 'a', -1) |
+-----------------------------+
|                             |
+-----------------------------+

select substring_index("abca", "a", -2);
+-----------------------------+
| split_part('abca', 'a', -2) |
+-----------------------------+
| bc                          |
+-----------------------------+
```

## keyword

substring_index
