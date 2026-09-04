# Native Geo Contract

- Status: active
- Owner: ViktorGo86
- Last Updated: 2026-09-04

## Summary

Introduce native geospatial support as a sequence of independently reviewable
contracts. `GEOGRAPHY` and `GEOMETRY` remain distinct logical SQL types with no
implicit conversion, while sharing a physical vectorized representation backed
by canonical OGC WKB bytes.

The first implementation step defines the descriptor and transport contract
only. It does not expose SQL syntax, enable native-table persistence, or change
legacy `VARCHAR` spatial functions and `GeoShape` payloads.

## Contract Sequence

1. Define `GeoTypeDesc` and `GeoStorageDesc`, including compatible Thrift and
   protobuf representations and BE round-trip behavior.
2. Introduce `GeoColumn`, backed by `BinaryColumn`, without embedding an
   engine-specific geometry object in the persisted or exchanged payload.
3. Propagate descriptors through FE types, expression results, exchange, spill,
   intermediate materialization, and vectorized serde.
4. Expose `GEOGRAPHY` with OGC:CRS84 longitude/latitude semantics and explicit
   WKT/WKB boundaries.
5. Add feature-gated native-table persistence and specify rolling-upgrade and
   downgrade behavior.
6. Add native spatial functions incrementally.
7. Enable `GEOMETRY` after its planar semantics and GEOS execution contract are
   agreed.

## Descriptor Contract

`GeoTypeDesc` separates semantic identity from physical representation:

- logical type: `GEOGRAPHY` or `GEOMETRY`;
- coordinate system: `SPHERICAL` or `CARTESIAN`;
- edge algorithm: `GEODESIC` or `LINEAR`;
- CRS identifier string;
- optional parsed SRID.

`GeoStorageDesc` describes the transported or persisted bytes:

- encoding: canonical OGC WKB initially;
- declared dimension: `UNKNOWN`, `XY`, `XYZ`, `XYM`, `XYZM`, or `MIXED`;
- producer validation state: `UNKNOWN`, `UNVALIDATED`, structurally validated,
  or semantically validated.

Unknown enum values occupy ordinal zero. All newly added Thrift and protobuf
fields are optional, use new ordinals, and must survive Thrift and protobuf
round trips. Descriptor values participate in type identity so spherical and
planar values cannot become assignable merely because their WKB bytes share the
same physical layout.

## Representation Invariants

- Canonical WKB is the only initial persistent and interchange payload.
- WKB bytes never determine whether a value has planar or spherical semantics.
- S2, GEOS, H3, and legacy `GeoShape` internal bytes are never persisted as
  native geo values.
- Legacy spatial values are not implicitly reinterpreted as native geo values.
- OGC:CRS84 is the initial `GEOGRAPHY` coordinate reference system: X is
  longitude, Y is latitude, and edges are spherical.
- EPSG:4326 or EWKB SRID 4326 may be accepted only at explicit SQL boundaries
  and canonicalized to the native contract; authority-axis ambiguity must not
  be silently propagated.

## Upgrade And Downgrade Direction

Native geo remains feature-gated until every participating FE, BE, and CN can
preserve its descriptors. Before downgrading to a version without native geo,
users must explicitly convert native values to canonical WKB in `VARBINARY`,
remove remaining native-geo metadata, finish or cancel related schema changes,
and create and synchronize a compatible FE metadata image.

## Acceptance Criteria: Descriptor Contract

- Thrift and protobuf schemas define semantic and storage descriptors using
  optional fields without reusing existing ordinals.
- BE `TypeDescriptor` preserves both descriptors through Thrift and protobuf
  round trips.
- Descriptor differences participate in equality and assignability.
- Existing scalar descriptors without geo metadata retain their behavior.
- Focused BE type tests pass.
- Generated-schema compatibility and BE module-boundary checks pass.

## Decision Log

- 2026-09-04: Use separate logical types with one canonical WKB representation.
- 2026-09-04: Deliver descriptor, column, transport, SQL, persistence, and
  execution semantics as separate contracts.
- 2026-09-04: Expose `GEOGRAPHY` first and keep `GEOMETRY` feature-gated until
  its planar/GEOS contract is ready.
