---
displayed_sidebar: docs
---

# ifnull

## 説明

`expr1` が NULL の場合、`expr2` を返します。`expr1` が NULL でない場合、`expr1` を返します。

## 構文

```Haskell
ifnull(expr1,expr2);
```

## パラメータ

`expr1` と `expr2` はデータ型が互換性がある必要があります。

## 戻り値

戻り値は `expr1` と同じ型です。

## 例

```Plain Text
mysql> select ifnull(2,4);
+--------------+
| ifnull(2, 4) |
+--------------+
|            2 |
+--------------+

mysql> select ifnull(NULL,2);
+-----------------+
| ifnull(NULL, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.01 sec)
```