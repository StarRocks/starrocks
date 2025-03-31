---
displayed_sidebar: docs
---

# cardinality

配列内の要素数を返します。結果の型は INT です。入力パラメータが NULL の場合、結果も NULL になります。NULL 要素も長さにカウントされます。

これは [array_length()](array_length.md) の別名です。

この関数は v3.0 以降でサポートされています。

## 構文

```Haskell
INT cardinality(any_array)
```

## パラメータ

`any_array`: 要素数を取得したい ARRAY 値。

## 戻り値

INT 値を返します。

## 例

```plain text
mysql> select cardinality([1,2,3]);
+-----------------------+
|  cardinality([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select cardinality([1,2,3,null]);
+------------------------------+
| cardinality([1, 2, 3, NULL]) |
+------------------------------+
|                            4 |
+------------------------------+

mysql> select cardinality([[1,2], [3,4]]);
+-----------------------------+
|  cardinality([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## キーワード

CARDINALITY, ARRAY_LENGTH, ARRAY