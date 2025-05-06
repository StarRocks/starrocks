---
displayed_sidebar: docs
---

# uuid

## 説明

VARCHAR 型のランダムな UUID を返します。この関数を2回呼び出すと、異なる2つの番号を生成することができます。UUID は36文字の長さで、5つの16進数が4つのハイフンで接続された aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee 形式です。

## 構文

```Haskell
uuid();
```

## パラメータ

なし

## 戻り値

VARCHAR 型の値を返します。

## 例

```Plain Text
mysql> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 74a2ed19-9d21-4a99-a67b-aa5545f26454 |
+--------------------------------------+
1 row in set (0.01 sec)
```