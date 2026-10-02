---
displayed_sidebar: docs
description: "Returns 1 if two H3 cell indexes share an edge (are grid neighbors)."
---

# h3IndexesAreNeighbors

Returns 1 if the two given H3 cell indexes share an edge, meaning they are direct grid neighbors. Both cells must be at the same resolution.

## Syntax

```Haskell
BOOLEAN h3IndexesAreNeighbors(BIGINT idx1, BIGINT idx2)
```

## Parameters

- `idx1`: The first H3 cell index. Supported data type: BIGINT.
- `idx2`: The second H3 cell index. Supported data type: BIGINT.

## Return value

Returns 1 (true) if the two cells share an edge, or 0 (false) if they do not. Returns NULL if either argument is NULL or is not a valid H3 cell index at the same resolution.

## Examples

```sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359);
+---------------------------------------------------------------+
| h3IndexesAreNeighbors(617420388351344639, 617420388352655359) |
+---------------------------------------------------------------+
|                                                             1 |
+---------------------------------------------------------------+
```

## keyword

H3INDEXESARENEIGHBORS,H3,SPATIAL
