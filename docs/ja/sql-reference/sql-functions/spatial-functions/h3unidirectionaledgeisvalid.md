---
displayed_sidebar: docs
description: "指定した値が有効なH3有向（単方向）辺インデックスである場合に1を返します。"
---

# h3UnidirectionalEdgeIsValid

指定した値が有効なH3有向（単方向）辺インデックスである場合に1を返します。有向辺は、特定の起点と終点を持つ2つの隣接H3セル間の共有境界をエンコードします。

## Syntax

```Haskell
BOOLEAN h3UnidirectionalEdgeIsValid(BIGINT edge)
```

## パラメータ

- `edge`: 検証するH3有向辺インデックス。サポートされるデータ型: BIGINT。

## 戻り値

有効なH3有向辺インデックスの場合は1（true）、そうでない場合は0（false）を返します。引数がNULLの場合はNULLを返します。

## 例

```sql
SELECT h3UnidirectionalEdgeIsValid(1248204388774707199);
+--------------------------------------------------+
| h3UnidirectionalEdgeIsValid(1248204388774707199) |
+--------------------------------------------------+
|                                                1 |
+--------------------------------------------------+
```

## キーワード

H3UNIDIRECTIONALEDGEISVALID,H3,SPATIAL
