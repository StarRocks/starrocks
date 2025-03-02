---
displayed_sidebar: docs
---

# translate

文字列内の指定された文字を置換します。この関数は、文字列 (`source`) を入力として受け取り、`source` 内の `from_string` 文字を `to_string` に置き換えます。

この関数は v3.2 からサポートされています。

## Syntax

```Haskell
TRANSLATE(source, from_string, to_string)
```

## Parameters

- `source`: `VARCHAR` 型をサポートします。翻訳される元の文字列です。`source` 内の文字が `from_string` に見つからない場合、その文字は結果の文字列にそのまま含まれます。

- `from_string`: `VARCHAR` 型をサポートします。`from_string` 内の各文字は、`to_string` の対応する文字に置き換えられるか、対応する文字がない場合（つまり、`to_string` の文字数が `from_string` より少ない場合）、その文字は結果の文字列から除外されます。例 2 と 3 を参照してください。`from_string` に同じ文字が複数回現れる場合、最初の出現のみが有効です。例 5 を参照してください。

- `to_string`: `VARCHAR` 型をサポートします。文字を置き換えるために使用される文字列です。`to_string` に `from_string` より多くの文字が指定されている場合、`to_string` の余分な文字は無視されます。例 4 を参照してください。

## Return value

`VARCHAR` 型の値を返します。

結果が `NULL` となるシナリオ:

- 入力パラメータのいずれかが `NULL` である場合。

- 翻訳後の結果文字列の長さが `VARCHAR` の最大長（1048576）を超える場合。

## Examples

```plaintext
-- 元の文字列の 'ab' を '12' に置き換えます。
mysql > select translate('abcabc', 'ab', '12') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- 元の文字列の 'mf1' を 'to' に置き換えます。'to' は 'mf1' より文字数が少なく、'1' は結果の文字列から除外されます。
mysql > select translate('s1m1a1rrfcks','mf1','to') as test;
+-----------+
| test      |
+-----------+
| starrocks |
+-----------+

-- 元の文字列の 'ab' を '1' に置き換えます。'1' は 'ab' より文字数が少なく、'b' は結果の文字列から除外されます。
mysql > select translate('abcabc', 'ab', '1') as test;
+------+
| test |
+------+
| 1c1c |
+------+

-- 元の文字列の 'ab' を '123' に置き換えます。'123' は 'ab' より文字数が多く、'3' は無視されます。
mysql > select translate('abcabc', 'ab', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- 元の文字列の 'aba' を '123' に置き換えます。'a' は2回現れますが、最初の出現のみが置き換えられます。
mysql > select translate('abcabc', 'aba', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- repeat() と concat() を使用してこの関数を使用します。結果の文字列が VARCHAR の最大長を超え、NULL が返されます。
mysql > select translate(concat('bcde', repeat('a', 1024*1024-3)), 'a', 'z') as test;
+--------+
| test   |
+--------+
| NULL   |
+--------+

-- length()、repeat()、concat() と共にこの関数を使用して、結果の文字列の長さを計算します。
mysql > select length(translate(concat('bcd', repeat('a', 1024*1024-3)), 'a', 'z')) as test;
+---------+
| test    |
+---------+
| 1048576 |
+---------+
```

## See also

- [concat](./concat.md)
- [length](./length.md)
- [repeat](./repeat.md)