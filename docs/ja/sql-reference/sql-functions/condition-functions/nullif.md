---
displayed_sidebar: docs
---

# nullif

## 説明

`expr1` が `expr2` と等しい場合、NULL を返します。それ以外の場合は、`expr1` を返します。

## 構文

```Haskell
nullif(expr1,expr2);
```

## パラメータ

`expr1` と `expr2` はデータ型が互換性のあるものでなければなりません。

## 戻り値

戻り値は `expr1` と同じ型になります。

## 例

```Plain Text
mysql> select nullif(1,2);
+--------------+
| nullif(1, 2) |
+--------------+
|            1 |
+--------------+

mysql> select nullif(1,1);
+--------------+
| nullif(1, 1) |
+--------------+
|         NULL |
+--------------+
```