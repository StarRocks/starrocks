---
displayed_sidebar: docs
---

# multiply

引数の積を計算します。

## 構文

```Haskell
multiply(arg1, arg2)
```

### パラメータ

`arg1`: 数値のソース列またはリテラル。  
`arg2`: 数値のソース列またはリテラル。

## 戻り値

2 つの引数の積を返します。戻り値の型は引数に依存します。

## 使用上の注意

非数値の値を指定すると、この関数は失敗します。

## 例

```Plain
MySQL [test]> select multiply(10,2);
+-----------------+
| multiply(10, 2) |
+-----------------+
|              20 |
+-----------------+
1 row in set (0.01 sec)

MySQL [test]> select multiply(1,2.1);
+------------------+
| multiply(1, 2.1) |
+------------------+
|              2.1 |
+------------------+
1 row in set (0.01 sec)

MySQL [test]> select * from t;
+------+------+------+------+
| id   | name | job1 | job2 |
+------+------+------+------+
|    2 |    2 |    2 |    2 |
+------+------+------+------+
1 row in set (0.08 sec)

MySQL [test]> select multiply(1.0,id) from t;
+-------------------+
| multiply(1.0, id) |
+-------------------+
|                 2 |
+-------------------+
1 row in set (0.01 sec)
```

## キーワード

multiply