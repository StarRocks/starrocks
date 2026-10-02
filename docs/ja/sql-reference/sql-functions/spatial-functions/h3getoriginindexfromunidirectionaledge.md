---
displayed_sidebar: docs
description: "有向（単方向）辺の起点H3セルインデックスを返します。"
---

# h3GetOriginIndexFromUnidirectionalEdge

有向（単方向）辺の起点H3セルインデックスを返します。有向辺は2つの隣接セルを接続し、起点はその辺が出発するセルです。

## Syntax

```Haskell
BIGINT h3GetOriginIndexFromUnidirectionalEdge(BIGINT edge)
```

## パラメータ

- `edge`: 有効なH3有向辺インデックス。サポートされるデータ型: BIGINT。

## 戻り値

有向辺の起点セルインデックスをBIGINTで返します。引数がNULL、またはインデックスが有効なH3有向辺でない場合はNULLを返します。

## 例

```sql
SELECT h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197);
+-------------------------------------------------------------+
| h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197) |
+-------------------------------------------------------------+
| 599686042433355773                                          |
+-------------------------------------------------------------+
```

## キーワード

H3GETORIGININDEXFROMUNIDIRECTIONALEDGE,H3,SPATIAL
