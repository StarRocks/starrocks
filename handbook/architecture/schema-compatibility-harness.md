# Schema Compatibility Harness

The generated-schema harness enforces thrift and protobuf compatibility rules mechanically for changes under `gensrc/`.

## Source of Truth

- Checker: `build-support/check_gensrc_schema_compatibility.py`
- Tests: `build-support/test_check_gensrc_schema_compatibility.py`
- Reviewed exceptions: `build-support/schema_compatibility_waivers.json`
- Schema sources: `gensrc/thrift/*.thrift`, `gensrc/proto/*.proto`

## Commands

```bash
python3 -m unittest build-support/test_check_gensrc_schema_compatibility.py
python3 build-support/check_gensrc_schema_compatibility.py --mode changed --base origin/main
python3 build-support/check_gensrc_schema_compatibility.py --mode full --base origin/main
```

## Working Rules

- New thrift struct fields and thrift RPC params or throws entries must be explicit `optional`.
- New proto2 fields must be `optional` or `repeated`.
- New proto3 singular fields must use explicit `optional`; `repeated` and `map` remain allowed.
- Field-number changes, type changes, and cardinality changes fail the harness.
- Field deletions require a checked-in waiver entry with matching signature, rationale, and owner.
- Remove stale waivers as soon as the compatibility exception is no longer needed.

## External Parquet Definitions

`gensrc/thrift/parquet.thrift` remains selected in both changed and full checks.
The checker permits only the exact append of upstream `LogicalType` alternatives
17 (`GeometryType GEOMETRY`) and 18 (`GeographyType GEOGRAPHY`), without changing
the pre-existing union body. This is not general Thrift union support: other union
edits still fail closed, and parsed structs are checked independently even when
an unsupported construct changes.

The four upstream-required `BoundingBox` X/Y fields have field-specific
`new_field_must_be_optional` waivers. These do not waive deletion, renumbering, or
type changes. Remove these addition waivers once the comparison base contains the
fields, as required by the stale-waiver check.
