---
displayed_sidebar: docs
---

# substring_index

## Description

デリミタの `count` 回出現する前または後の部分文字列を抽出します。

- `count` が正の場合、文字列の先頭からカウントが始まり、この関数は `count` 番目のデリミタの前の部分文字列を返します。例えば、`select substring_index('https://www.starrocks.io', '.', 2);` は、2 番目の `.` デリミタの前の部分文字列 `https://www.starrocks` を返します。

- `count` が負の場合、文字列の末尾からカウントが始まり、この関数は `count` 番目のデリミタの後の部分文字列を返します。例えば、`select substring_index('https://www.starrocks.io', '.', -2);` は、2 番目の `.` デリミタの後の部分文字列 `starrocks.io` を返します。

いずれかの入力パラメータが null の場合、NULL が返されます。

この関数は v3.2 からサポートされています。

## Syntax

```Haskell
VARCHAR substring_index(VARCHAR str, VARCHAR delimiter, INT count)
```

## Parameters

- `str`: 必須、分割したい文字列。
- `delimiter`: 必須、文字列を分割するために使用するデリミタ。
- `count`: 必須、デリミタの位置。この値は 0 にできません。それ以外の場合、NULL が返されます。値が文字列内の実際のデリミタの数より大きい場合、文字列全体が返されます。

## Return value

VARCHAR 値を返します。

## Examples

```Plain Text
-- 2 番目の "." デリミタの前の部分文字列を返します。
mysql> select substring_index('https://www.starrocks.io', '.', 2);
+-----------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', 2) |
+-----------------------------------------------------+
| https://www.starrocks                               |
+-----------------------------------------------------+

-- count が負の場合。
mysql> select substring_index('https://www.starrocks.io', '.', -2);
+------------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', -2) |
+------------------------------------------------------+
| starrocks.io                                         |
+------------------------------------------------------+

mysql> select substring_index("hello world", " ", 1);
+----------------------------------------+
| substring_index("hello world", " ", 1) |
+----------------------------------------+
| hello                                  |
+----------------------------------------+

mysql> select substring_index("hello world", " ", -1);
+-----------------------------------------+
| substring_index('hello world', ' ', -1) |
+-----------------------------------------+
| world                                   |
+-----------------------------------------+

-- count が 0 の場合、NULL が返されます。
mysql> select substring_index("hello world", " ", 0);
+----------------------------------------+
| substring_index('hello world', ' ', 0) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+

-- count が文字列内のスペースの数より大きい場合、文字列全体が返されます。
mysql> select substring_index("hello world", " ", 2);
+----------------------------------------+
| substring_index("hello world", " ", 2) |
+----------------------------------------+
| hello world                            |
+----------------------------------------+

-- count が文字列内のスペースの数より大きい場合、文字列全体が返されます。
mysql> select substring_index("hello world", " ", -2);
+-----------------------------------------+
| substring_index("hello world", " ", -2) |
+-----------------------------------------+
| hello world                             |
+-----------------------------------------+
```

## keyword

substring_index