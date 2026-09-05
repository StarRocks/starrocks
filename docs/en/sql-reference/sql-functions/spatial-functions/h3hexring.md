---
displayed_sidebar: docs
description: "Returns the hollow ring of H3 cells at exactly grid distance k from the origin."
---

# h3HexRing

Returns the cells at exactly grid distance k from the origin cell (a hollow ring), unlike `h3kRing` which returns a filled disk. The result does not include the origin or any cells closer than distance k. The ring may be empty or an error may occur if a pentagon is encountered along the ring path.

## Syntax

```Haskell
ARRAY<BIGINT> h3HexRing(BIGINT h3index, INT k)
```

## Parameters

- `h3index`: The origin H3 cell index. Supported data type: BIGINT.
- `k`: The exact grid distance of the ring. Must be a non-negative integer. Supported data type: INT.

## Return value

Returns an `ARRAY<BIGINT>` of H3 cell indexes at exactly grid distance k from the origin. Returns NULL if either argument is NULL or `h3index` is not a valid H3 cell index. Returns an empty array or raises an error if a pentagon blocks the ring traversal.

## Examples

```sql
SELECT h3HexRing(590080540275638271, 1);
+------------------------------------------------------------------------------------------------------------------+
| h3HexRing(590080540275638271, 1)                                                                                 |
+------------------------------------------------------------------------------------------------------------------+
| [590077447899185151,590077585338138623,590079509483487231,590080471556161535,590080677714591743,590080815153545215] |
+------------------------------------------------------------------------------------------------------------------+
```

## keyword

H3HEXRING,H3,SPATIAL
