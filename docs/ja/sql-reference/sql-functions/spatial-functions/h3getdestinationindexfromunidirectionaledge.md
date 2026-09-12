---
displayed_sidebar: docs
description: "有向（単方向）辺の終点H3セルインデックスを返します。"
---

# h3GetDestinationIndexFromUnidirectionalEdge

有向（単方向）辺の終点H3セルインデックスを返します。有向辺は2つの隣接セルを接続し、終点はその辺が指し示すセルです。

## Syntax

```Haskell
BIGINT h3GetDestinationIndexFromUnidirectionalEdge(BIGINT edge)
```

## パラメータ

- `edge`: 有効なH3有向辺インデックス。サポートされるデータ型: BIGINT。

## 戻り値

有向辺の終点セルインデックスをBIGINTで返します。引数がNULL、またはインデックスが有効なH3有向辺でない場合はNULLを返します。

## 例

```sql
SELECT h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197);
+------------------------------------------------------------------+
| h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197) |
+------------------------------------------------------------------+
| 599686043507097597                                               |
+------------------------------------------------------------------+
```

## キーワード

H3GETDESTINATIONINDEXFROMUNIDIRECTIONALEDGE,H3,SPATIAL
