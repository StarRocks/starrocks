---
displayed_sidebar: docs
---

# substring_index

## Description

デリミタの`count`回の出現の前または後にある部分文字列を抽出します。

- `count`が正の場合、文字列の先頭からカウントが始まり、この関数は`count`番目のデリミタの前にある部分文字列を返します。例えば、`select substring_index('https://www.starrocks.io', '.', 2);`は、2番目の`.`デリミタの前の部分文字列である`https://www.starrocks`を返します。

- `count`が負の場合、文字列の末尾からカウントが始まり、この関数は`count`番目のデリミタの後にある部分文字列を返します。例えば、`select substring_index('https://www.starrocks.io', '.', -2);`は、2番目の`.`デリミタの後の部分文字列である`starrocks.io`を返します。

入力パラメータがnullの場合、NULLが返されます。

この関数はv3.2からサポートされています。

## Syntax

```Haskell
VARCHAR substring_index(VARCHAR str, VARCHAR delimiter, INT count)
```

## Parameters

- `str`: 必須、分割したい文字列。
- `delimiter`: 必須、文字列を分割するために使用されるデリミタ。
- `count`: 必須、デリミタの位置。この値は0にできません。そうでなければ、NULLが返されます。値が文字列中の実際のデリミタの数より大きい場合、全体の文字列が返されます。

## Return value

VARCHAR値を返します。

## Examples

```Plain Text
-- 2番目の"."デリミタの前にある部分文字列を返します。
mysql> select substring_index('https://www.starrocks.io', '.', 2);
+-----------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', 2) |
+-----------------------------------------------------+
| https://www.starrocks                               |
+-----------------------------------------------------+

-- countが負の場合。
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

-- countが0でNULLが返されます。
mysql> select substring_index("hello world", " ", 0);
+----------------------------------------+
| substring_index('hello world', ' ', 0) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+

-- countが文字列中のスペースの数より大きく、全体の文字列が返されます。
mysql> select substring_index("hello world", " ", 2);
+----------------------------------------+
| substring_index("hello world", " ", 2) |
+----------------------------------------+
| hello world                            |
+----------------------------------------+

-- countが文字列中のスペースの数より大きく、全体の文字列が返されます。
mysql> select substring_index("hello world", " ", -2);
+-----------------------------------------+
| substring_index("hello world", " ", -2) |
+-----------------------------------------+
| hello world                             |
+-----------------------------------------+
```

## keyword

substring_index