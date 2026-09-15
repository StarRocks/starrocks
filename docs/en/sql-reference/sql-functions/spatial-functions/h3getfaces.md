---
displayed_sidebar: docs
description: "Returns the icosahedron face numbers intersected by the given H3 cell."
---

# h3GetFaces

Returns the icosahedron face numbers (0–19) that are intersected by the given H3 cell. H3 uses a gnomonic projection from an icosahedron; most cells lie entirely on a single face, but cells near face boundaries can span two or more faces.

## Syntax

```Haskell
ARRAY<INT> h3GetFaces(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns an `ARRAY<INT>` of icosahedron face numbers (0–19) intersected by the cell. Returns NULL if the argument is NULL or is not a valid H3 cell index.

## Examples

```sql
SELECT h3GetFaces(599686042433355775);
+--------------------------------+
| h3GetFaces(599686042433355775) |
+--------------------------------+
| [7]                            |
+--------------------------------+
```

## keyword

H3GETFACES,H3,SPATIAL
