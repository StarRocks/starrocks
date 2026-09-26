---
displayed_sidebar: docs
description: "H3セルインデックスの解像度（0〜15）を返します。"
---

# h3GetResolution

指定したH3セルインデックスの解像度を返します。H3解像度は0（最粗、約4,250 kmの辺長）から15（最細、約0.5 mの辺長）の範囲です。

## Syntax

```Haskell
INT h3GetResolution(BIGINT h3index)
```

## パラメータ

- `h3index`: H3セルインデックス。サポートされるデータ型: BIGINT。

## 戻り値

セルの解像度を表す[0, 15]の範囲のINTを返します。引数がNULL、またはインデックスが有効なH3セルでない場合はNULLを返します。

## 例

```sql
SELECT h3GetResolution(617700169958293503);
+-------------------------------------+
| h3GetResolution(617700169958293503) |
+-------------------------------------+
|                                   9 |
+-------------------------------------+
```

## キーワード

H3GETRESOLUTION,H3,SPATIAL,RESOLUTION
