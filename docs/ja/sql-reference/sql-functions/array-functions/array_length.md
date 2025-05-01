---
displayed_sidebar: docs
---

# array_length

## 説明

配列内の要素数を返します。結果の型は INT です。入力パラメータが NULL の場合、結果も NULL になります。NULL 要素も長さにカウントされます。

別名として [cardinality()](cardinality.md) があります。

## 構文

```Haskell
INT array_length(any_array)
```

## パラメータ

`any_array`: 要素数を取得したい ARRAY 値。

## 戻り値

INT 値を返します。

## 例

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

## キーワード

ARRAY_LENGTH, ARRAY, CARDINALITY