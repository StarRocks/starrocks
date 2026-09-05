---
displayed_sidebar: docs
description: "BIGINT値が有効なH3セルインデックスかどうかを返します。"
---

# h3IsValid

指定したBIGINT値が有効なH3セルインデックスかどうかを返します。NULL以外の入力に対してはNULLを返しません。

## Syntax

```Haskell
BOOLEAN h3IsValid(BIGINT h3index)
```

## パラメータ

- `h3index`: H3セルインデックスとして検証する値。サポートされるデータ型: BIGINT。

## 戻り値

BOOLEANを返します。有効なH3セルインデックスの場合は`true`（1）、そうでない場合は`false`（0）を返します。引数がNULLの場合のみNULLを返します。

## 例

```sql
SELECT h3IsValid(617700169958293503);
+-------------------------------+
| h3IsValid(617700169958293503) |
+-------------------------------+
|                             1 |
+-------------------------------+

SELECT h3IsValid(0);
+--------------+
| h3IsValid(0) |
+--------------+
|            0 |
+--------------+
```

## キーワード

H3ISVALID,H3,SPATIAL,VALID
