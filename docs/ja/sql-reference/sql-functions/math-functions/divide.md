---
displayed_sidebar: docs
---

# divide

## 説明

x を y で割った商を返します。y が 0 の場合は null を返します。

## 構文

```Haskell
divide(x, y)
```

### パラメータ

- `x`: サポートされている型は DOUBLE、FLOAT、LARGEINT、BIGINT、INT、SMALLINT、TINYINT、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128 です。

- `y`: サポートされている型は `x` と同じです。

## 戻り値

DOUBLE データ型の値を返します。

## 使用上の注意

数値以外の値を指定した場合、この関数は `NULL` を返します。

## 例

```Plain Text
mysql> select divide(3, 2);
+--------------+
| divide(3, 2) |
+--------------+
|          1.5 |
+--------------+
1 row in set (0.00 sec)
```