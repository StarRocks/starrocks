---
displayed_sidebar: docs
description: "Stores two-dimensional OGC geometry values in native WKB form."
---

# GEOMETRY

`GEOMETRY` stores two-dimensional geometry values in canonical OGC Well-Known Binary (WKB) form.

The following OGC geometry families are supported:

- `POINT`
- `LINESTRING`
- `POLYGON`
- `MULTIPOINT`
- `MULTILINESTRING`
- `MULTIPOLYGON`
- `GEOMETRYCOLLECTION`

Each family also supports `EMPTY` values. Use [ST_GeomFromText](../../sql-functions/spatial-functions/st_geometryfromtext.md) or [ST_GeomFromWKB](../../sql-functions/spatial-functions/st_geometryfromwkb.md) to construct a value, and use [ST_AsText](../../sql-functions/spatial-functions/st_astext.md) or [ST_AsBinary](../../sql-functions/spatial-functions/st_asbinary.md) to serialize it.

## Example

```SQL
CREATE TABLE geometry_example (
    id BIGINT NOT NULL,
    shape GEOMETRY NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id);

INSERT INTO geometry_example VALUES
    (1, ST_GeomFromText('POINT (1 2)')),
    (2, ST_GeomFromText('MULTIPOLYGON (((0 0, 0 2, 2 2, 0 0)))')),
    (3, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'));

SELECT id, ST_AsText(shape) FROM geometry_example ORDER BY id;
```

## Limitations

- Only two-dimensional OGC WKT and WKB are supported. EWKT/EWKB, SRID, and Z/M ordinates are not supported.
- A `GEOMETRY` column cannot be a key, sort key, distribution key, or partition key.
- Comparison, grouping, ordering, and arithmetic operations are not supported for `GEOMETRY` values.
- Only `NULL` is supported as a column default value.
