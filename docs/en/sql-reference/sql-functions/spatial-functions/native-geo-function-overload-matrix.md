---
displayed_sidebar: docs
description: "Lists the native GEOGRAPHY and GEOMETRY overloads, function IDs, supported inputs, and runtime behavior."
---

# Native GEO function overload matrix

This page defines the overload contract for the native `GEOGRAPHY` and `GEOMETRY` functions introduced in the initial GEO function set. Function IDs are part of FE-to-BE dispatch. Overloads are resolved by the logical SQL type; the runtime validates the value descriptor separately and never infers spherical or planar semantics from WKB bytes.

## Common behavior

- Native constructors and serializers support all seven two-dimensional OGC families: `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and `GEOMETRYCOLLECTION`. They also support typed `EMPTY` values and empty children.
- A `NULL` argument produces `NULL`. Constructors also return `NULL` for malformed WKT/WKB or unsupported constructor parameters.
- Native compute functions require an XY value and a valid descriptor. Unsupported families, dimensions, or descriptors produce a controlled error unless a row-specific rule below says that `EMPTY` produces `NULL`.
- `GEOGRAPHY` and `GEOMETRY` are distinct logical types. Mixed-kind calls have no overload and are rejected.
- `GEOGRAPHY` uses `OGC:CRS84`, spherical edges, longitude/latitude coordinate order, and SRID 4326. `GEOMETRY` uses planar semantics and the explicit CRS stored in its descriptor. No function in this matrix performs reprojection.

## Constructors

| Signature | Function ID | Result | Families and dimensions | CRS and descriptor behavior |
| --- | ---: | --- | --- | --- |
| `ST_GeogFromText(VARCHAR wkt)` | 120020 | `GEOGRAPHY` | All seven 2D families and `EMPTY` | Uses the native `OGC:CRS84` spherical descriptor and SRID 4326. |
| `ST_GeogFromText(VARCHAR wkt, INT srid)` | 120021 | `GEOGRAPHY` | All seven 2D families and `EMPTY` | `srid` must be 4326; the result uses the native `OGC:CRS84` spherical descriptor. |
| `ST_GeomFromText(VARCHAR wkt, VARCHAR crs)` | 120022 | `GEOMETRY` | All seven 2D families and `EMPTY` | `crs` must be a non-empty string literal and becomes the planar result descriptor. There is no default CRS. |
| `ST_GeogFromWKB(VARBINARY wkb)` | 120030 | `GEOGRAPHY` | All seven 2D families and `EMPTY`; either WKB byte order | Uses the native `OGC:CRS84` spherical descriptor and SRID 4326. EWKB is unsupported. |
| `ST_GeogFromWKB(VARBINARY wkb, INT srid)` | 120031 | `GEOGRAPHY` | All seven 2D families and `EMPTY`; either WKB byte order | `srid` must be 4326; the result uses the native `OGC:CRS84` spherical descriptor. EWKB is unsupported. |
| `ST_GeomFromWKB(VARBINARY wkb, VARCHAR crs)` | 120032 | `GEOMETRY` | All seven 2D families and `EMPTY`; either WKB byte order | `crs` must be a non-empty string literal and becomes the planar result descriptor. EWKB and Z/M ordinates are unsupported. |

Geography coordinates must satisfy the supported longitude and latitude ranges. Geometry constructors do not reinterpret coordinates and do not transform them into another CRS. See [ST_GeogFromText](st_geogfromtext.md), [ST_GeogFromWKB](st_geogfromwkb.md), [ST_GeomFromText](st_geometryfromtext.md), and [ST_GeomFromWKB](st_geomfromwkb.md).

## Serializers

| Signature | Function ID | Result | Behavior |
| --- | ---: | --- | --- |
| `ST_AsText(GEOGRAPHY value)` | 120040 | `VARCHAR` | Emits WKT without changing coordinates or the input descriptor. |
| `ST_AsText(GEOMETRY value)` | 120041 | `VARCHAR` | Emits WKT without changing coordinates or the input descriptor. |
| `ST_AsWKT(GEOGRAPHY value)` | 120050 | `VARCHAR` | Alias of the native `ST_AsText(GEOGRAPHY)` behavior. |
| `ST_AsWKT(GEOMETRY value)` | 120051 | `VARCHAR` | Alias of the native `ST_AsText(GEOMETRY)` behavior. |
| `ST_AsBinary(GEOGRAPHY value)` | 120060 | `VARBINARY` | Emits WKB. Logical kind, CRS, and edge semantics remain type metadata and are not encoded as EWKB. |
| `ST_AsBinary(GEOMETRY value)` | 120061 | `VARBINARY` | Emits WKB. Logical kind and CRS remain type metadata and are not encoded as EWKB. |
| `ST_AsWKB(GEOGRAPHY value)` | 120070 | `VARBINARY` | Alias of the native `ST_AsBinary(GEOGRAPHY)` behavior. |
| `ST_AsWKB(GEOMETRY value)` | 120071 | `VARBINARY` | Alias of the native `ST_AsBinary(GEOMETRY)` behavior. |

See [ST_AsText and ST_AsWKT](st_astext.md) and [ST_AsBinary and ST_AsWKB](st_asbinary.md).

## Initial compute functions

| Signature | Function ID | Result and units | Supported values | `NULL`, `EMPTY`, and descriptor behavior |
| --- | ---: | --- | --- | --- |
| `ST_X(GEOGRAPHY point)` | 120080 | `DOUBLE`, longitude in degrees | Non-empty XY `POINT` | `NULL` propagates. `EMPTY`, non-`POINT`, or an unsupported descriptor is an error. |
| `ST_X(GEOMETRY point)` | 120081 | `DOUBLE`, input CRS units | Non-empty XY `POINT` | `NULL` propagates. `EMPTY`, non-`POINT`, or an unsupported descriptor is an error. |
| `ST_Y(GEOGRAPHY point)` | 120090 | `DOUBLE`, latitude in degrees | Non-empty XY `POINT` | `NULL` propagates. `EMPTY`, non-`POINT`, or an unsupported descriptor is an error. |
| `ST_Y(GEOMETRY point)` | 120091 | `DOUBLE`, input CRS units | Non-empty XY `POINT` | `NULL` propagates. `EMPTY`, non-`POINT`, or an unsupported descriptor is an error. |
| `ST_GeometryType(GEOGRAPHY value)` | 120170 | `VARCHAR` family name | Every supported XY family | `NULL` propagates. A typed `EMPTY` keeps its family name. Unsupported dimensions or descriptors are errors. |
| `ST_GeometryType(GEOMETRY value)` | 120171 | `VARCHAR` family name | Every supported XY family | `NULL` propagates. A typed `EMPTY` keeps its family name. Unsupported dimensions or descriptors are errors. |
| `ST_Distance(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120180 | `DOUBLE`, meters | XY `POINT`/`POINT` under the spherical CRS84 contract | `NULL` or `EMPTY` produces `NULL`. Unsupported families, dimensions, or descriptors are errors. |
| `ST_Distance(GEOMETRY lhs, GEOMETRY rhs)` | 120181 | `DOUBLE`, input CRS units | XY `POINT`/`POINT` with matching descriptors | `NULL` or `EMPTY` produces `NULL`. Unsupported families, dimensions, or incompatible descriptors are errors. |

See [ST_X](st_x.md), [ST_Y](st_y.md), [ST_GeometryType](st_geometrytype.md), and [ST_Distance](st_distance.md).

## Legacy compatibility

The native overloads do not renumber or replace the existing `VARCHAR` functions:

| Legacy signature | Function ID |
| --- | ---: |
| `ST_X(VARCHAR)` | 120001 |
| `ST_Y(VARCHAR)` | 120002 |
| `ST_AsText(VARCHAR)` | 120004 |
| `ST_AsWKT(VARCHAR)` | 120005 |
| `ST_GeometryFromText(VARCHAR)` | 120006 |
| `ST_GeomFromText(VARCHAR)` | 120007 |

## Upgrade and reference-test contract

For a rolling upgrade, upgrade BEs before FEs. These functions use the ordinary stable function-ID dispatch and do not introduce a separate GEO version gate. A newer FE with an older BE is not a supported upgrade order.

The contract is enforced by focused FE analyzer tests for overload resolution, return types, legacy compatibility, and function IDs, and by BE registry tests for every native ID. Existing BE function tests cover constant, nullable, varying, `EMPTY`, malformed-input, family, dimension, CRS, and descriptor behavior.
