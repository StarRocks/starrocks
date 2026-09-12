---
displayed_sidebar: docs
description: "Returns 1 if the given value is a valid H3 directed (unidirectional) edge index."
---

# h3UnidirectionalEdgeIsValid

Returns 1 if the given value is a valid H3 directed (unidirectional) edge index. A directed edge encodes the shared boundary between two neighbouring H3 cells, with a specific origin and destination.

## Syntax

```Haskell
BOOLEAN h3UnidirectionalEdgeIsValid(BIGINT edge)
```

## Parameters

- `edge`: An H3 directed edge index to validate. Supported data type: BIGINT.

## Return value

Returns 1 (true) if the value is a valid H3 directed edge index, or 0 (false) otherwise. Returns NULL if the argument is NULL.

## Examples

```sql
SELECT h3UnidirectionalEdgeIsValid(1248204388774707199);
+--------------------------------------------------+
| h3UnidirectionalEdgeIsValid(1248204388774707199) |
+--------------------------------------------------+
|                                                1 |
+--------------------------------------------------+
```

## keyword

H3UNIDIRECTIONALEDGEISVALID,H3,SPATIAL
