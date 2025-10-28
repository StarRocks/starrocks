---
displayed_sidebar: docs
---

# split_part



This function splits a given string according to the separators and returns the requested part. (start counting from the beginning)

## Syntax

```Haskell
VARCHAR split_part(VARCHAR content, VARCHAR delimiter, INT field)
```

## Parameters

`content`: The string to be split. Data type: VARCHAR.

`delimiter`: The separator used to split the string. Data type: VARCHAR.

`field`: The position of the part to return. Positive values count from the beginning, negative values count from the end. Data type: INT.

## Return Value

Returns the specified part of the split string. Data type: VARCHAR.

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

MySQL > select split_part("hello world", "|", 1);
+----------------------------------+
| split_part('hello world', '|', 1) |
+----------------------------------+
| hello world                      |
+----------------------------------+

MySQL > select split_part("hello world", "|", -1);
+-----------------------------------+
| split_part('hello world', '|', -1) |
+-----------------------------------+
| hello world                       |
+-----------------------------------+

MySQL > select split_part("hello world", "|", 2);
+----------------------------------+
| split_part('hello world', '|', 2) |
+----------------------------------+
|                                  |
+----------------------------------+

MySQL > select split_part("abca", "a", 1);
+----------------------------+
| split_part('abca', 'a', 1) |
+----------------------------+
|                            |
+----------------------------+

MySQL > select split_part("abca", "a", -1);
+-----------------------------+
| split_part('abca', 'a', -1) |
+-----------------------------+
|                             |
+-----------------------------+

MySQL > select split_part("abca", "a", -2);
+-----------------------------+
| split_part('abca', 'a', -2) |
+-----------------------------+
| bc                          |
+-----------------------------+

MySQL > select split_part("2019年7月8日", "月", 1);
+-----------------------------------------+
| split_part('2019年7月8日', '月', 1)     |
+-----------------------------------------+
| 2019年7                                 |
+-----------------------------------------+
```

## keyword

SPLIT_PART,SPLIT,PART