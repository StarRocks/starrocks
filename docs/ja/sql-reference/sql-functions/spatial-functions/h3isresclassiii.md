---
displayed_sidebar: docs
description: "H3セルの解像度がClass III（奇数解像度1,3,5,7,9,11,13,15）の場合に1を返します。"
---

# h3IsResClassIII

指定したH3セルの解像度がClass IIIである場合に1を返します。H3解像度はClass II（偶数: 0,2,4,6,8,10,12,14）とClass III（奇数: 1,3,5,7,9,11,13,15）が交互に並び、Class IIIグリッドはClass IIグリッドに対して19.1°回転しています。

## Syntax

```Haskell
BOOLEAN h3IsResClassIII(BIGINT h3index)
```

## パラメータ

- `h3index`: H3セルインデックス。サポートされるデータ型: BIGINT。

## 戻り値

セルがClass III解像度の場合は1（true）、Class II解像度の場合は0（false）を返します。引数がNULL、またはインデックスが有効なH3セルでない場合はNULLを返します。

## 例

```sql
SELECT h3IsResClassIII(617420388352917503);
+--------------------------------------+
| h3IsResClassIII(617420388352917503)  |
+--------------------------------------+
|                                    1 |
+--------------------------------------+
```

## キーワード

H3ISRESCLASSIII,H3,SPATIAL
