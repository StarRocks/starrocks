---
displayed_sidebar: docs
---

# uuid

VARCHAR 型のランダムな UUID を返します。この関数を2回呼び出すと、異なる番号を生成することがあります。UUID は 36 文字の長さで、5 つの 16 進数が aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee 形式で4つのハイフンで接続されています。

## Syntax

```Haskell
uuid();
```

## Parameters

なし

## Return value

VARCHAR 型の値を返します。

## Examples

```Plain Text
mysql> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 74a2ed19-9d21-4a99-a67b-aa5545f26454 |
+--------------------------------------+
1 row in set (0.01 sec)
```