---
displayed_sidebar: docs
---

# array_length

配列内の要素数を返します。結果の型は INT です。入力パラメータが NULL の場合、結果も NULL になります。NULL 要素も長さに含まれます。

[cardinality()](cardinality.md) という別名があります。

## Syntax

```Haskell
INT array_length(any_array)
```

## Parameters

`any_array`: 要素数を取得したい ARRAY 値。

## Return value

INT 値を返します。

## Examples

```plain text
mysql> select array_length([1,2,3]);
+-----------------------+
| array_length([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select array_length([1,2,3,null]);
+-------------------------------+
| array_length([1, 2, 3, NULL]) |
+-------------------------------+
|                             4 |
+-------------------------------+

mysql> select array_length([[1,2], [3,4]]);
+-----------------------------+
| array_length([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## keywords

ARRAY_LENGTH, ARRAY, CARDINALITY