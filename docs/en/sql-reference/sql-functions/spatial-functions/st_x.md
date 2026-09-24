---
displayed_sidebar: docs
description: "Returns the X coordinate of a point. For GEOGRAPHY, X is longitude in degrees."
---

# ST_X

Returns the X coordinate of a point.

For a `GEOGRAPHY` value, X is longitude in degrees under the OGC:CRS84 spherical contract. The value must be a non-empty `POINT`.

## Syntax

```SQL
DOUBLE ST_X(VARCHAR point)
DOUBLE ST_X(GEOGRAPHY point)
```

## Return value

Returns `NULL` when the input is `NULL`. For `GEOGRAPHY`, an empty value or a non-`POINT` family produces an error. Unsupported dimensions or descriptors also produce an error.

## Examples

```SQL
SELECT ST_X(ST_Point(24.7, 56.7));
-- 24.7

SELECT ST_X(ST_GeogFromText('POINT (24.7 56.7)'));
-- 24.7
```

## keyword

ST_X,ST,X
