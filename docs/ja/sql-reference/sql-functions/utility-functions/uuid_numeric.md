---
displayed_sidebar: docs
---

# uuid_numeric

ランダムな LARGEINT 型の UUID を返します。この関数は `uuid` 関数に比べて 2 桁優れた実行性能を持っています。

## 構文

```Haskell
uuid_numeric();
```

## パラメータ

なし

## 戻り値

LARGEINT 型の値を返します。

## 例

```Plain Text
MySQL > select uuid_numeric();
+--------------------------+
| uuid_numeric()           |
+--------------------------+
| 558712445286367898661205 |
+--------------------------+
1 row in set (0.00 sec)
```