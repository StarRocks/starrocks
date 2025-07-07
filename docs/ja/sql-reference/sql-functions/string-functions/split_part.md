---
displayed_sidebar: docs
---

# split_part

この関数は、指定された文字列をセパレーターに従って分割し、要求された部分を返します。（最初から数え始めます）

## Syntax

```Haskell
VARCHAR split_part(VARCHAR content, VARCHAR delimiter, INT field)
```

## Parameters

`content`: 分割する文字列。データ型: VARCHAR。

`delimiter`: 文字列を分割するために使用されるセパレーター。データ型: VARCHAR。

`field`: 返す部分の位置。正の値は先頭から、負の値は末尾からカウントします。データ型: INT。

## Return Value

分割された文字列の指定された部分を返します。データ型: VARCHAR。

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