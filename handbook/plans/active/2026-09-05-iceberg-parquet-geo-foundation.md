# Iceberg/Parquet Geo Compatibility Foundation

- Status: active
- Owner: Viktor Gnidenko
- Last Updated: 2026-09-06

## Summary

Implement Milestone 1 / Contract 1.3 of [#78693](https://github.com/StarRocks/starrocks/issues/78693).
Contracts 1.1 (#78700) and 1.2 (#78701) are merged. This branch starts at main
b0d3704064856091a2f51c906d3d10922f23c386 and contains only the dependent validation
and disabled-read integration.

No native SQL types, GeoColumn, geometry execution, normalization, native storage,
GeoJSON rendering, new enablement flag, or writes are introduced. Both native geo
kinds remain disabled. WKB bytes are not inspected to infer semantics.

## Implementation

- Validate Parquet geo annotations before constructing the schema: BYTE_ARRAY
  leaves only, no conflicting logical/converted annotations, nonempty explicit CRS.
- Match Iceberg fields by ID when present, otherwise by case-aware name. Validate
  logical kind, exact CRS identifier and the five distinct geographic edge algorithms.
  Geometry edges are PLANAR. Absent Parquet parameters default to CRS84/SPHERICAL.
- Reject malformed/unknown metadata explicitly rather than silently defaulting it.
- Reintroduce optional source iceberg_type with an actual BE consumer: distinguish
  an explicit ordinary Iceberg type from an older sender lacking geo metadata.
  The value is external schema identity, never a StarRocks SQL type or capability.
- Reject projected geo before missing-column substitution, split planning, min/max,
  dictionary, bloom and geo-bound pruning. Preserve supported-column reads.
- Protect native Parquet readers, direct column-reader construction and Arrow FILES()
  inference/explicit schemas, including empty files and nested geo containers.

## Acceptance Criteria

- [x] Base contains both merged prerequisites.
- [x] Adapt prior local prototype to enum kind and reviewed external definitions.
- [x] Positive/negative schema and physical annotation matrix.
- [x] Source type consumer, old-sender compatibility and FE transport tests.
- [x] Real plain/dictionary Parquet fixtures: supported-column read; geo rejection.
- [x] Missing/renamed/case-insensitive/nested column and empty-file guards.
- [x] FE targeted suite, full format_test and connector_file_test under ASAN.
- [x] Schema/BE boundary/handbook/style checks and final diff review.

## Observability

Reviewed FileReader initialization and FormatScannerStats pruning counters, plus
Arrow reader initialization exception/status handling. Controlled errors identify
the column and failure category without logging geometry payloads. Existing scan
profiles and status propagation are retained; rejected scans do not count pruning
attempts. No new metric is needed for a disabled feature. Tests check existing
statistics_tried_counter and bloom_filter_tried_counter remain zero on rejection.

## Decision Log

- 2026-09-06: Start Contract 1.3 from main after #78700 and #78701 merged.
- 2026-09-06: Restore optional iceberg_type only with a concrete schema-conflict
  validation consumer. Missing metadata on older senders is not an ordinary type.
- 2026-09-06: Keep both geo kinds disabled; do not introduce a partially implemented
  capability gate. Reject projections before pruning or missing-column substitution.

## Verification

Fresh verification against this branch (2026-09-06):

- FE: 242 tests passed across two successful runs, zero failures/errors/skips,
  including checkstyle and full
  production/test compilation. Suites: IcebergGeoMetadataTest (4),
  IcebergUnsupportedTypeQueryTest (5), IcebergApiConverterTest (43),
  ColumnTypeConverterTest (14).
- Extended FE suites: IcebergTableTest (14), IcebergScanNodeTest (37), and
  IcebergMetadataTest (125). The first extended run hit 17 Mockito instrumentation
  errors because Byte Buddy does not officially support the local Java 21 runtime.
  All 176 tests passed with JAVA_TOOL_OPTIONS=-Dnet.bytebuddy.experimental=true,
  as suggested by Byte Buddy's diagnostic; no source/dependency changes were made.
- Schema checker: all 35 unit tests and full comparison against the base passed.
- BE format_test: 390 tests in 64 suites passed under ASAN, including all 13
  GeoMetadataTest cases. The final run rebuilt the latest tests after correcting
  an invalid zero-column fixture to a schema with id but without shape.
- BE connector_file_test: 227 tests in 18 suites passed under ASAN. This includes
  empty-file inference/projection rejection, both native geo annotations, ordinary
  column reads, and rejection of explicit binary/string geo projections. The first
  run exposed a shared Arrow-wrapper lifetime error in the new test; each reader
  now owns a separate wrapper, and the complete suite passed on rerun.
- BE boundaries, generated AGENTS consistency, handbook structure and diff whitespace
  checks passed. CI clang-format-10 reports no replacements in all 12 changed C++
  files.
- Earlier prototype results are not acceptance evidence for this revision. Tests
  using in-memory real Parquet files and FE mock catalogs are not a live external
  Iceberg catalog test.

The plan remains active until the focused Contract 1.3 PR is reviewed and merged.
No Milestone 2/3 functionality is enabled by these changes.

```bash
./run-fe-ut.sh -j 2 --test IcebergGeoMetadataTest,IcebergUnsupportedTypeQueryTest,IcebergApiConverterTest,ColumnTypeConverterTest
JAVA_TOOL_OPTIONS=-Dnet.bytebuddy.experimental=true ./run-fe-ut.sh -j 2 --test IcebergTableTest,IcebergScanNodeTest,IcebergMetadataTest
./run-be-ut.sh -j 6 --build-target format_test --module format_test --without-java-ext --without-paimon-cpp --without-tenann
./run-be-ut.sh -j 6 --build-target connector_file_test --module connector_file_test --without-java-ext --without-paimon-cpp --without-tenann
python3 -m unittest discover -s build-support -p test_check_gensrc_schema_compatibility.py
python3 build-support/check_gensrc_schema_compatibility.py --mode full --base b0d3704064856091a2f51c906d3d10922f23c386
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/check_repo_handbook.py
```
