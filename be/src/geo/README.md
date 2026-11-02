# geo

This document describes the validity rules for the native GEO types (GEOGRAPHY / GEOMETRY) in the BE. It applies to
`be/src/geo/`, `be/src/exprs/geo_functions.cpp`, `be/src/exprs/h3_functions.cpp`, and any other code that reads GEO
coordinates.

## Two kinds of validity

### Structural validity

Whether a row's WKB can be decoded safely and unambiguously:

- the byte order is 0 or 1;
- the type code is a supported ISO type code;
- counts and lengths fit the buffer, with no trailing bytes;
- polygon rings are closed and have at least 4 positions;
- coordinates are finite (not NaN / Inf), with one exception: a POINT whose X and Y are both NaN is the WKB
  encoding of `POINT EMPTY`. `WkbCodec::parse_wkb` accepts it and `WkbCodec::to_wkb` writes it. A POINT with only
  one NaN ordinate, or a NaN in any other geometry, is invalid. A fast path that cannot handle `POINT EMPTY` must
  fall back to the full parser instead of rejecting it (as `decode_canonical_xy_point` does);
- GEOGRAPHY longitudes and latitudes are within CRS84 ranges.

**The dimension is part of the structure.** The thousands digit of the type code (for example 1001 = POINT Z) decides
how many doubles each position has, and therefore how many bytes each point takes. Without reading the type code, a
decoder cannot find the next point. Compute functions currently support XY only; a row with Z or M is treated as
structurally invalid.

### Topological validity

Whether the decoded geometry is a valid OGC simple feature, for example:

- rings are simple and do not self-intersect;
- holes lie inside their shell and do not overlap each other;
- the polygon interior is connected.

**Coverage validity** across rows (polygon interiors do not overlap, shared edges have matching vertices) belongs to
the same category.

## Rules

1. **Every function that reads coordinates checks structural validity per row, from the row's own WKB.**
   This is mandatory: skipping it can misread coordinates or read out of bounds. Use `WkbCodec::parse_wkb`,
   `inspect_geo_wkb`, or a fast path that checks the header first (for example `decode_canonical_xy_point`).
   A structurally invalid row makes a compute function return an error; constructors (`ST_GeomFromText/WKB`,
   `ST_GeogFromText/WKB`) turn unparseable input into NULL instead.

2. **Column-level storage metadata is not a substitute for the per-row check.**
   `storage.dimension` and `storage.validation_state` are claims made by the producer of the data and are not enforced
   on writes (`append_wkb*` never inspects the bytes). Do not skip the per-row check because a column is labelled XY or
   validated. Boundary checks may reject explicitly unsupported labels (`XYZ` / `XYM` / `XYZM`) to report a clear
   `NotSupported`, but must admit `UNKNOWN` and `MIXED`. For example, columns produced by Iceberg GEOGRAPHY scans are
   `UNKNOWN`.

3. **Topological validity is not a precondition of general functions.**
   Functions that only read a geometry (coordinate accessors, output, measurements such as area and length, distance)
   compute on whatever geometry they receive. With invalid topology the result may be meaningless, but the function
   must never crash or cause undefined behavior.

4. **Only functions whose algorithm requires valid topology check it, before computing, and return
   `InvalidArgument` on invalid input.**
   Examples: the overlays (`ST_Intersection` / `ST_Union` / `ST_Difference` / `ST_SymDifference`), `ST_Buffer`,
   `ST_SimplifyPreserveTopology`, `H3_PolygonToCells`, and the GEOGRAPHY predicates that build polygons through S2.
   New functions of this kind must check up front in the same way.

5. **Coverage validity is the user's responsibility.**
   `ST_CoverageSimplify` keeps its structural and safety checks (ring closure, degenerate rings, zero-area rings, the
   number of owners of a shared edge), but does not validate that the input is a valid coverage. Invalid input may
   produce artifacts in the output.

6. **Users check topology explicitly with `ST_IsValid`.**
   It returns a boolean and does not fail on topologically invalid input.

## When adding or changing a compute function

- Decode each row's own WKB; never admit or reject rows based on the column's storage labels.
- Add the function to `GeoNonXYPayloadTest` in `be/test/exprs/geography_functions_test.cpp`: an XY control input must
  compute, and Z / M inputs must be rejected even when the column is labelled `XY` and `SEMANTICALLY_VALIDATED`.
- If the algorithm requires valid topology, add a test showing that topologically invalid input returns
  `InvalidArgument` instead of a result.
