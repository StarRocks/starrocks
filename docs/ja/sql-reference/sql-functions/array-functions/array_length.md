---
displayed_sidebar: docs
---

# array_length

## 説明

配列内の要素数を返します。結果の型は INT です。パラメータが NULL の場合、結果も NULL になります。

## 構文

```Haskell
array_length(any_array)
```

## 例

```plain text
mysql> select array_length([1,2,3]);
+-----------------------+
| array_length([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select array_length([[1,2], [3,4]]);
+-----------------------------+
| array_length([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## キーワード

ARRAY_LENGTH, ARRAY