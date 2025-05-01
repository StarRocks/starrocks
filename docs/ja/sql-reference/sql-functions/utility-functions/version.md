---
displayed_sidebar: docs
---

# version

## 説明

現在の MySQL データベースのバージョンを返します。

StarRocks のバージョンを照会するには、 [current_version](current_version.md) を使用できます。

## 構文

```Haskell
VARCHAR version();
```

## パラメーター

なし

## 戻り値

VARCHAR 型の値を返します。

## 例

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```

## 参照

[current_version](../utility-functions/current_version.md)