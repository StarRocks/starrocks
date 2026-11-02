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
   This is mandatory: skipping it can misread coordinates or read out of bounds. The helpers check different things:

   - **Full validation: `WkbCodec::parse_wkb`** with the semantics of the column (`GEOGRAPHY_CRS84` or
     `GEOMETRY_CARTESIAN`). It enforces every rule above: XY type codes only, lengths, ring closure and minimum
     positions, finite coordinates (with the `POINT EMPTY` exception), and CRS84 ranges for GEOGRAPHY. This is the
     reference a kernel must match.
   - **Layout inspection: `inspect_geo_wkb`.** It walks headers and counts and bounds-checks lengths, and reports the
     geometry type and dimension. It does **not** enforce the contract: it accepts Z/M payloads, and it skips coordinate
     arrays without checking finiteness, CRS84 ranges or ring constraints. Use it only to look at a row's layout, and
     then reject what the kernel does not support (for example `info.dimension != GEO_DIMENSION_XY`) and still run
     full validation before using coordinates.
   - **Fast paths** that read bytes directly (for example `decode_canonical_xy_point`) must enforce themselves
     everything `parse_wkb` would for the rows they accept: the exact canonical header (byte order and XY type code),
     the exact length, finite coordinates, and, for GEOGRAPHY, CRS84 ranges (`is_valid_crs84_coordinate`). Any row a
     fast path does not fully handle, including `POINT EMPTY`, falls back to `parse_wkb`; a fast path may only accept
     rows that `parse_wkb` accepts, with the same coordinates.

   A structurally invalid row makes a compute function return an error; constructors (`ST_GeomFromText/WKB`,
   `ST_GeogFromText/WKB`) turn unparseable input into NULL instead.

2. **Column-level storage metadata is not a substitute for the per-row check.**
   `storage.dimension` and `storage.validation_state` are claims made by the producer of the data and are not enforced
   on writes (`append_wkb*` never inspects the bytes). Do not skip the per-row check because a column is labelled XY or
   validated.

   Boundary checks may reject explicitly unsupported labels (`XYZ` / `XYM` / `XYZM`) to report a clear `NotSupported`.
   `check_geography_boundary`, `check_geometry_boundary` and the `ST_CoverageSimplify` window also admit `UNKNOWN` and
   `MIXED`; for example, columns produced by Iceberg GEOGRAPHY scans are `UNKNOWN`. Two gates are current exceptions:
   `check_geometry_compute_boundary` (GEOMETRY compute functions) and `h3_geo_input` (H3 functions) require an `XY`
   label and return `NotSupported` for other labels, even when every row is XY. Admitting `UNKNOWN` and `MIXED` in every
   kernel is the target contract (tracked in #82086); new boundary checks must not add further `XY`-only gates.

3. **Topological validity is decided per kernel, not by the decoder.**
   `parse_wkb` checks structure only. Each kernel either requires valid topology or does not; a kernel that does not
   check it must still never crash or cause undefined behavior on topologically invalid input.

4. **Kernels that require valid topology check it before computing and return `InvalidArgument` on invalid input.**
   Today this includes:
   - the measurements `ST_Area` / `ST_Length` / `ST_Perimeter`, for both GEOMETRY and GEOGRAPHY
     (`planar_measurement` / `spherical_measurement` in `geo_measurements.cpp`);
   - `ST_Centroid` (`planar_centroid` / `spherical_centroid`);
   - the overlays (`ST_Intersection` / `ST_Union` / `ST_Difference` / `ST_SymDifference`), `ST_Buffer` and
     `ST_SimplifyPreserveTopology`;
   - `H3_PolygonToCells`;
   - GEOGRAPHY kernels that build S2 polygons or lines (`prepare_spherical_polygon`,
     `prepare_spherical_line_family`), such as the GEOGRAPHY containment and intersection predicates.

   Kernels that only access coordinates or serialize a geometry (`ST_X` / `ST_Y`, `ST_GeometryType`, `ST_AsText` /
   `ST_AsBinary`) do not check topology. A new kernel must state which group it belongs to; if it requires valid
   topology, check it up front with `planar_is_valid` / `spherical_is_valid` (or the equivalent preparation) and test
   that invalid input returns `InvalidArgument`.

5. **Coverage validity is the user's responsibility.**
   `ST_CoverageSimplify` keeps its structural and safety checks (ring closure, degenerate rings, zero-area rings, the
   number of owners of a shared edge), but does not validate that the input is a valid coverage. Invalid input may
   produce artifacts in the output.

6. **Users check topology explicitly with `ST_IsValid`.**
   It returns a boolean and does not fail on topologically invalid input.

## When adding or changing a compute function

- Decode each row's own WKB, and never admit a row because of the column's storage labels. Do not reject columns by
  label beyond what the shared boundary checks already do (explicit `XYZ` / `XYM` / `XYZM` labels, plus the current
  `XY`-only exceptions described in rule 2).
- Add the function to `GeoNonXYPayloadTest` in `be/test/exprs/geography_functions_test.cpp`: an XY control input must
  compute, and Z / M inputs must be rejected even when the column is labelled `XY` and `SEMANTICALLY_VALIDATED`.
- If the algorithm requires valid topology, add a test showing that topologically invalid input returns
  `InvalidArgument` instead of a result.
