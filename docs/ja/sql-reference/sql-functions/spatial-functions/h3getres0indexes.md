---
displayed_sidebar: docs
description: "すべての122個のH3解像度0基底セルインデックスを返します。"
---

# h3GetRes0Indexes

すべての122個のH3解像度0基底セルインデックスを返します。これらはH3階層で最も粗いセルであり、グリッドのトップレベルを形成します。

## Syntax

```Haskell
ARRAY<BIGINT> h3GetRes0Indexes()
```

## 戻り値

すべての122個の解像度0基底セルインデックスを含む`ARRAY<BIGINT>`を返します。

## 例

```sql
SELECT array_length(h3GetRes0Indexes());
+-----------------------------------+
| array_length(h3GetRes0Indexes())  |
+-----------------------------------+
| 122                               |
+-----------------------------------+
```

## キーワード

H3GETRES0INDEXES,H3,SPATIAL
