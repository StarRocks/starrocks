---
displayed_sidebar: docs
description: "Returns the center child H3 cell of the given index at a finer resolution."
---

# h3ToCenterChild

Returns the center child H3 cell of the given index at a specified finer resolution. Each H3 cell contains 7 children at the next finer resolution; this function returns the one whose center is closest to the parent's center.

## Syntax

```Haskell
BIGINT h3ToCenterChild(BIGINT h3index, INT resolution)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.
- `resolution`: The target child resolution, which must be greater than or equal to the resolution of `h3index`. Supported data type: INT.

## Return value

Returns a BIGINT representing the center child cell index at the specified resolution. Returns NULL if either argument is NULL, `resolution` is less than the cell's resolution, or `h3index` is not a valid H3 cell index.

## Examples

```sql
SELECT h3ToCenterChild(577023702256844799, 1);
+----------------------------------------+
| h3ToCenterChild(577023702256844799, 1) |
+----------------------------------------+
| 581496515558637567                     |
+----------------------------------------+
```

## keyword

H3TOCENTERCHILD,H3,SPATIAL
