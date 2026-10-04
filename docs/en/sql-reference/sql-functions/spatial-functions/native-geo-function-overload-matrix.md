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
| `ST_Distance(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120180 | `DOUBLE`, meters | XY `POINT`/`POINT`, or `POINT` with `LINESTRING`/`MULTILINESTRING`, under the spherical CRS84 contract | `NULL` or `EMPTY` produces `NULL`. Unsupported families, dimensions, descriptors, or ambiguous antipodal line segments are errors. |
| `ST_Distance(GEOMETRY lhs, GEOMETRY rhs)` | 120181 | `DOUBLE`, input CRS units | XY `POINT`/`POINT`, or `POINT` with `LINESTRING`/`MULTILINESTRING`, with matching descriptors | `NULL` or `EMPTY` produces `NULL`. Unsupported families, dimensions, or incompatible descriptors are errors. |
| `ST_DWithin(GEOGRAPHY lhs, GEOGRAPHY rhs, DOUBLE distance)` | 120230 | `BOOLEAN`; `distance` in meters | XY `POINT` with `LINESTRING`/`MULTILINESTRING`, in either order | Inclusive threshold. `NULL` propagates; `EMPTY` returns `false`. The threshold must be finite and nonnegative. |
| `ST_DWithin(GEOMETRY lhs, GEOMETRY rhs, DOUBLE distance)` | 120231 | `BOOLEAN`; `distance` in input CRS units | XY `POINT` with `LINESTRING`/`MULTILINESTRING`, in either order, with matching descriptors | Inclusive threshold. `NULL` propagates; `EMPTY` returns `false`. The threshold must be finite and nonnegative. |

See [ST_X](st_x.md), [ST_Y](st_y.md), [ST_GeometryType](st_geometrytype.md), [ST_Distance](st_distance.md), and [ST_DWithin](st_dwithin.md).

## Containment predicates

| Signature | Function ID | Boundary behavior |
| --- | ---: | --- |
| `ST_Contains(GEOGRAPHY polygon, GEOGRAPHY point)` | 120190 | Returns `true` only for a point in the polygon interior. |
| `ST_Contains(GEOMETRY polygon, GEOMETRY point)` | 120191 | Returns `true` only for a point in the polygon interior. |
| `ST_Within(GEOGRAPHY point, GEOGRAPHY polygon)` | 120200 | Converse of `ST_Contains`; excludes the boundary. |
| `ST_Within(GEOMETRY point, GEOMETRY polygon)` | 120201 | Converse of `ST_Contains`; excludes the boundary. |
| `ST_Covers(GEOGRAPHY polygon, GEOGRAPHY point)` | 120210 | Includes exterior and interior-ring boundaries. |
| `ST_Covers(GEOMETRY polygon, GEOMETRY point)` | 120211 | Includes exterior and interior-ring boundaries. |
| `ST_CoveredBy(GEOGRAPHY point, GEOGRAPHY polygon)` | 120220 | Converse of `ST_Covers`; includes the boundary. |
| `ST_CoveredBy(GEOMETRY point, GEOMETRY polygon)` | 120221 | Converse of `ST_Covers`; includes the boundary. |

These overloads support an XY `POINT` with a `POLYGON` or `MULTIPOLYGON`. `GEOGRAPHY` evaluates spherical CRS84 edges; `GEOMETRY` evaluates planar edges and requires matching descriptors. `NULL` propagates, while an `EMPTY` input returns `false`. Unsupported families, dimensions, descriptors, and mixed `GEOGRAPHY`/`GEOMETRY` calls are rejected. See [ST_Contains](st_contains.md), [ST_Within](st_within.md), [ST_Covers](st_covers.md), and [ST_CoveredBy](st_coveredby.md).

## Intersection predicate

| Signature | Function ID | Boundary behavior |
| --- | ---: | --- |
| `ST_Intersects(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120240 | Spherical overlap, containment, and boundary contact return `true`. |
| `ST_Intersects(GEOMETRY lhs, GEOMETRY rhs)` | 120241 | Planar overlap, containment, and boundary contact return `true`. |

These overloads support XY `POLYGON` and `MULTIPOLYGON` values. `GEOMETRY` inputs require matching descriptors. `NULL` propagates, while an `EMPTY` input returns `false`. Unsupported families, collections, and mixed native logical types are rejected. See [ST_Intersects](st_intersects.md).

## Measurements and properties

| Signature | Function ID | Result and units | Family behavior |
| --- | ---: | --- | --- |
| `ST_Area(GEOGRAPHY value)` | 120250 | `DOUBLE`, square meters | Sums polygon area recursively; holes subtract; lower dimensions contribute zero. |
| `ST_Area(GEOMETRY value)` | 120251 | `DOUBLE`, squared input CRS units | Sums polygon area recursively; holes subtract; lower dimensions contribute zero. |
| `ST_Length(GEOGRAPHY value)` | 120260 | `DOUBLE`, meters | Sums line length recursively; points and polygons contribute zero. |
| `ST_Length(GEOMETRY value)` | 120261 | `DOUBLE`, input CRS units | Sums line length recursively; points and polygons contribute zero. |
| `ST_Perimeter(GEOGRAPHY value)` | 120270 | `DOUBLE`, meters | Sums exterior and interior polygon ring lengths recursively. |
| `ST_Perimeter(GEOMETRY value)` | 120271 | `DOUBLE`, input CRS units | Sums exterior and interior polygon ring lengths recursively. |
| `ST_Centroid(GEOGRAPHY value)` | 120280 | `GEOGRAPHY POINT`, same descriptor | Uses the highest non-empty dimension and spherical size weighting. |
| `ST_Centroid(GEOMETRY value)` | 120281 | `GEOMETRY POINT`, same descriptor | Uses the highest non-empty dimension and planar size weighting. |
| `ST_IsValid(GEOGRAPHY value)` | 120290 | `BOOLEAN` | Uses spherical validity, including overlapping MultiPolygon interiors. |
| `ST_IsValid(GEOMETRY value)` | 120291 | `BOOLEAN` | Uses robust planar topology validity. |

All overloads accept the seven XY OGC families and recurse through `MULTI*` and `GEOMETRYCOLLECTION` where applicable. `NULL` propagates. Measurements return zero for `EMPTY`; `ST_Centroid` returns `POINT EMPTY`; `ST_IsValid` returns `true`. Topology-invalid readable values return `false` only from `ST_IsValid`; measurements and centroid reject them. Malformed WKB and unsupported dimensions or descriptors are errors.

Spherical polygon rings are orientation-independent and normalized to their smaller region; a single ring does not represent more than a hemisphere. See [ST_Area](st_area.md), [ST_Length](st_length.md), [ST_Perimeter](st_perimeter.md), [ST_Centroid](st_centroid.md), and [ST_IsValid](st_isvalid.md).
## CRS metadata and transformation

| Signature | Function ID | Result | Behavior |
| --- | ---: | --- | --- |
| `ST_SRID(GEOMETRY value)` | 120300 | `INT` or `NULL` | Reads the numeric EPSG mapping from the descriptor; does not transform coordinates. |
| `ST_SetSRID(GEOMETRY value, INT constant)` | 120305 | `GEOMETRY`, target EPSG descriptor | Changes metadata only; WKB and coordinates are unchanged. |
| `ST_Transform(GEOMETRY value, INT constant)` | 120310 | `GEOMETRY`, target EPSG descriptor | Reprojects XY coordinates between EPSG:4326 and EPSG:3857 using Web Mercator formulas. |

The two target SRIDs must be FE-foldable constants equal to 4326 or 3857. Source CRS must be EPSG:4326, EPSG:3857, or OGC:CRS84. `ST_Transform` handles all seven XY OGC families and collections; `NULL` propagates and `EMPTY` stays empty. See [ST_SRID](st_srid.md), [ST_SetSRID](st_setsrid.md), and [ST_Transform](st_transform.md).

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
| `ST_Contains(VARCHAR, VARCHAR)` | 120014 |

## Upgrade and reference-test contract

For a rolling upgrade, upgrade BEs before FEs. Native scalar functions use the ordinary stable function-ID dispatch and do not introduce a separate GEO version gate. The coverage window registry is described below. A newer FE with an older BE is not a supported upgrade order.

The contract is enforced by focused FE analyzer tests for overload resolution, return types, legacy compatibility, and function IDs, and by BE registry tests for every native ID. Existing BE function tests cover constant, nullable, varying, `EMPTY`, malformed-input, family, dimension, CRS, and descriptor behavior.

## Polygon overlays

Native XY polygons and multipolygons with compatible CRS; Cartesian semantics in both 4326 and 3857. NULL propagates. For `ST_Union`, `ST_Difference`, and `ST_SymDifference`, one component returns POLYGON, multiple return MULTIPOLYGON, and empty returns POLYGON EMPTY. Invalid topology is an error. See the linked pages for precision and resource limits.

`ST_Intersection` also preserves shared edges and isolated contact points, returning line or point families when no area is present, or GEOMETRYCOLLECTION for mixed dimensions. Parts covered by higher-dimensional output are not repeated; disjoint inputs return POLYGON EMPTY.

| Signature | Function ID | Reference |
| --- | ---: | --- |
| `ST_Intersection(GEOMETRY, GEOMETRY)` | 120341 | [ST_Intersection](st_intersection.md) |
| `ST_Union(GEOMETRY, GEOMETRY)` | 120351 | [ST_Union](st_union.md) |
| `ST_Difference(GEOMETRY, GEOMETRY)` | 120361 | [ST_Difference](st_difference.md) |
| `ST_SymDifference(GEOMETRY, GEOMETRY)` | 120371 | [ST_SymDifference](st_symdifference.md) |

## Cartesian buffer

Native XY POINT, LINESTRING, POLYGON and their MULTI families; finite signed distance in input CRS units, round approximation, NULL propagation and polygonal EMPTY output. The result preserves CRS. GEOGRAPHY, collections, Z/M and options are unsupported by ST_Buffer.

| Signature | Function ID | Reference |
| --- | ---: | --- |
| `ST_Buffer(GEOMETRY, DOUBLE)` | 120401 | [ST_Buffer](st_buffer.md) |

## Topology preserving simplification

Cartesian XY scalar geometry with preserved families, rings, contacts, typed empty children and CRS. Tolerance uses input coordinate units. GEOGRAPHY, Z/M and cross-row coverage simplification are unsupported.

| Signature | Function ID | Reference |
| --- | ---: | --- |
| `ST_SimplifyPreserveTopology(GEOMETRY, DOUBLE)` | 120411 | [ST_SimplifyPreserveTopology](st_simplifypreservetopology.md) |

## H3 cell functions

These overloads require native XY CRS84 GEOGRAPHY for geometry arguments; the cell is a signed BIGINT. See [H3 functions](h3-functions.md) for validity, NULL/EMPTY, fill approximation, and limits.

| Signature | Function ID | Result |
| --- | ---: | --- |
| `H3_FromGeo(GEOGRAPHY, INT)` | 120500 | `BIGINT` |
| `H3_GridDisk(BIGINT, INT)` | 120501 | `ARRAY<BIGINT>` |
| `H3_ToParent(BIGINT, INT)` | 120502 | `BIGINT` |
| `H3_ToChildren(BIGINT, INT)` | 120503 | `ARRAY<BIGINT>` |
| `H3_Resolution(BIGINT)` | 120504 | `INT` |
| `H3_ToBoundary(BIGINT)` | 120505 | `GEOGRAPHY` |
| `H3_PolygonToCells(GEOGRAPHY, INT)` | 120506 | `ARRAY<BIGINT>` |

## Joint coverage simplification (window)

Both overloads process the whole partition jointly and return one XY GEOMETRY per row with the original family and CRS. Parameters must be foldable plan constants; OVER is required. No inner ORDER BY, explicit frame, ordinary aggregate or spill support. See [ST_CoverageSimplify](st_coveragesimplify.md) for topology, NULL/EMPTY, tolerance and resource limits.

| Signature | Function ID |
| --- | ---: |
| `ST_CoverageSimplify(GEOMETRY, DOUBLE) OVER (...)` | 120421 |
| `ST_CoverageSimplify(GEOMETRY, DOUBLE, BOOLEAN) OVER (...)` | 120431 |

GEOGRAPHY counterparts 120420 and 120430 are reserved without registration. FE records these IDs; BE resolves this window function by name and logical argument/result types through the window registry. Upgrade BEs before FEs.
