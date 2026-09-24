---
displayed_sidebar: docs
description: "Returns the spherical distance in meters between two GEOGRAPHY points."
---

# ST_DISTANCE

Returns the spherical distance in meters between two `GEOGRAPHY` points under the OGC:CRS84 contract.

## Syntax

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
```

## Return value

Both values must be `POINT`. Returns `NULL` if either input is `NULL` or EMPTY. A non-`POINT` family, unsupported dimension, or unsupported descriptor produces an error.

Only the `GEOGRAPHY` overload is currently supported. `GEOMETRY` inputs and broader geometry-family pairs are not supported.

## Examples

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 0)'),
    ST_GeogFromText('POINT (1 0)'));
-- approximately 111195.1 meters
```

## keyword

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY
