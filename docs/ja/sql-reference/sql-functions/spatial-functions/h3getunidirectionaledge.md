---
displayed_sidebar: docs
description: "2つの隣接セル間の有向H3辺インデックスを返します。"
---

# h3GetUnidirectionalEdge

起点セルから終点セルへの境界を表す有向（単方向）H3辺インデックスを返します。2つのセルは同じ解像度の直接隣接セルでなければなりません。

## Syntax

```Haskell
BIGINT h3GetUnidirectionalEdge(BIGINT origin, BIGINT destination)
```

## パラメータ

- `origin`: 起点H3セルインデックス。サポートされるデータ型: BIGINT。
- `destination`: 終点H3セルインデックス。サポートされるデータ型: BIGINT。

## 戻り値

起点セルから終点セルへの有向辺インデックスをBIGINTで返します。いずれかの引数がNULL、有効なH3セルインデックスでない、またはセルが隣接していない場合はNULLを返します。

## 例

```sql
SELECT h3GetUnidirectionalEdge(599686042433355775, 599686043507097599);
+-----------------------------------------------------------------+
| h3GetUnidirectionalEdge(599686042433355775, 599686043507097599) |
+-----------------------------------------------------------------+
| 1248204388774707199                                             |
+-----------------------------------------------------------------+
```

## キーワード

H3GETUNIDIRECTIONALEDGE,H3,SPATIAL
