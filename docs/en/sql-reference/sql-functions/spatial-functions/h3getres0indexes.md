---
displayed_sidebar: docs
description: "Returns all 122 H3 resolution-0 base cell indexes."
---

# h3GetRes0Indexes

Returns all 122 H3 resolution-0 base cell indexes. These are the coarsest cells in the H3 hierarchy and form the top level of the grid.

## Syntax

```Haskell
ARRAY<BIGINT> h3GetRes0Indexes()
```

## Return value

Returns an `ARRAY<BIGINT>` containing all 122 resolution-0 base cell indexes.

## Examples

```sql
SELECT array_length(h3GetRes0Indexes());
+-----------------------------------+
| array_length(h3GetRes0Indexes())  |
+-----------------------------------+
| 122                               |
+-----------------------------------+
```

## keyword

H3GETRES0INDEXES,H3,SPATIAL
