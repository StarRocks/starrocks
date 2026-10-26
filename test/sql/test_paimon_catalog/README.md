# Paimon catalog tests

From `test/`, run `python3 run.py -d sql/test_paimon_catalog -v`.
Configure the shared OSS bucket and credentials in `conf/sr.conf`. The BE needs
paimon-cpp.

## Test data

These cases read fixed tables from
`oss://${oss_bucket}/ci_catalog_test_data/paimon/paimon_test.db/`:
`scalar_types`, `variant_basic`, `image_blob`, `image_blob_desc`,
`image_blob_view`, and `pk_merge_v1`, plus:

| Additional table | Fixed data layout |
| --- | --- |
| `partitioned_v1` | Two string partitions and a NULL partition |
| `schema_evolution_v1` | Old/new files after rename, INT-to-BIGINT widening, drop/add |
| `nested_types_v1` | ARRAY, MAP, ROW and nested ARRAY, including empty/NULL values |
| `empty_v1` | No snapshots; never written |
| `empty_deleted_v1` | Snapshots containing inserts followed by deletion of all rows |
| `pk_sequence_v1` | Delete/reinsert and repeated updates across three commits |
| `writer_refresh_v1` | Final snapshot after an external update/insert |
| `append_parquet_v1`, `append_orc_v1` | Identical append-only rows in two formats, with explicit table/column statistics |
| `file_pruning_v1` | Two files with disjoint value ranges; no published column statistics |
| `pk_{parquet,orc}_{unmerged,compacted}_v1` | Identical merged rows before/after physical compaction, with explicit statistics |
| `append_avro_v1` | Avro append-only rows, including NULL values |
| `pk_{deduplicate,first_row,partial_update,aggregation}_{parquet,orc,avro}_v1` | Three write commits per table; first-row is compacted, other engines retain unmerged files |
| `global_index_v1` | Two rows with a B-tree global index and one later row outside index coverage |
| `pk_dv_{parquet,orc,avro}_v1` | 1,000 initial rows followed by update/delete/insert; actual deletion vectors retained |

The reader-boundary cases additionally use these fixed tables:

| Table | Boundary |
| --- | --- |
| `reader_json_v1`, `reader_per_level_v1` | JSON data files and an unsupported per-level format configuration |
| `reader_aggregate_product_v1` | Unsupported native `product` aggregation uses JNI |
| `reader_timestamp9_v1`, `reader_orc_legacy_v1` | Nanosecond timestamps and legacy ORC timestamp encoding |
| `reader_pk_nullable_map_orc_v1`, `reader_nullable_map_avro_v1` | Nullable MAP keys on SDK reads; the ORC table retains unmerged primary-key files |
| `reader_schema_cast_v1` | INT-to-STRING evolution outside native compatibility rules |
| `reader_bitmap64_dv_v1` | Real bitmap64 DV files plus INT-to-STRING evolution, preventing raw-file bypass of the SDK check |
| `reader_fallback_same_v1`, `reader_fallback_json_v1`, `reader_fallback_schema_v1` | Matching schemas, incompatible fallback format, and schema mismatch rejection |
| `reader_mixed_partitions_v1` | One scan reads a Parquet partition with raw files and an Avro partition with native |
| `reader_partial_groups_v1`, `reader_partial_retract_v1` | Independent sequence groups, composite ordering, late older updates, and field-level retraction |
| `reader_partial_delete_v1`, `reader_partial_ignore_delete_v1` | Partial-update whole-row removal and ignored deletes; NULL updates preserve prior fields |

The persistent warehouse is `oss://${oss_bucket}/ci_catalog_test_data/paimon/`. Upload the
complete `paimon_test.db` directory before running these cases, preserving all
schema, snapshot, manifest and data files:

```text
ci_catalog_test_data/paimon/
└── paimon_test.db/
    ├── scalar_types/
    ├── pk_merge_v1/
    └── ...
```

Do not add a `warehouse/` directory between `paimon/` and `paimon_test.db/`.
In the existing `[.oss]` subsection under `[env]` in `conf/sr.conf`, configure:

```ini
[env]
  [.oss]
    oss_bucket = starrocks-ci-test
    oss_endpoint = oss-ap-southeast-1.aliyuncs.com
    oss_region = ap-southeast-1
    oss_ak = <access-key-id>
    oss_sk = <access-key-secret>
```

Use an endpoint reachable from both FE and BE. CI supplies its own OSS settings;
keep credentials out of committed files. The original six fixtures originate from
[starrocks-testing-data](https://github.com/StarRocks/starrocks-testing-data/tree/4bce0eac1a87b23982561b7c53636d125e13abf2/catalog/paimon).

Cases create unique Catalog names and drop only those Catalogs when finished.
They do not upload, modify or delete the shared data. An early failure may leave
a Catalog requiring cleanup. Tests that generate or modify data must use a
separate `tmp/<case-name>/<UUID>/` prefix, not this persistent warehouse.
See [T/test_paimon_reader_modes](T/test_paimon_reader_modes) for an example.

## Reader assertions

After creating the catalog as in that example, add
these statements to a T file:

```sql
set paimon_reader_mode = 'native';
[ORDER]select id, amount, note from paimon_readers_${uuid0}.paimon_test.pk_merge_v1 order by id;
function: assert_paimon_reader("select id, amount, note from paimon_readers_${uuid0}.paimon_test.pk_merge_v1 order by id", "pk_merge_v1", "native")
function: assert_paimon_native_profile("select id, amount, note from paimon_readers_${uuid0}.paimon_test.pk_merge_v1 order by id")
```

- `assert_paimon_reader(query, table, expected)` checks FE routing with
  `TRACE VALUES EXTERNAL`. Use the table name without the catalog/database prefix.
  The expected reader must have a positive counter; the other readers must have
  zero counters. Missing counters or mixed routes fail the assertion.
- `assert_paimon_readers(query, table, expected, reasons="")` checks a complete
  comma-separated reader set, such as `"starrocks,native"`, and optional positive
  selection reasons, such as `"RAW_FILES,NATIVE_SUPPORTED"`. Unexpected readers,
  absent expected readers, and missing or zero reason counters fail the assertion.
- `assert_paimon_native_profile(query)` executes the query with synchronous
  profiling and requires a BE `PaimonNativeReader` section. Use it when expecting
  paimon-cpp execution. It restores the previous profile settings afterward.

| `expected` | Reader |
| --- | --- |
| `native` | paimon-cpp |
| `jni` | Java Paimon reader |
| `starrocks` | StarRocks native file reader |

For `paimon_reader_mode = 'auto'`, specify the reader expected for the fixture:
`starrocks` for `scalar_types`, or `native` for `pk_merge_v1`. There is no `auto`
assertion value. Use a query that scans actual data; an empty scan may have no
positive reader counter or reader profile section.

Each helper runs an additional query and returns `None` on success; its R-file
result block therefore contains `None`. Keep the ordinary SELECT and its expected
rows to verify results as well as the reader path.

## Coverage

| Cases | Coverage |
| --- | --- |
| `filesystem_catalog`, `reader_modes` | Scalar, NULL, VARIANT, BLOB and reader routing/profile |
| `scalar_predicates` | Projection order, NULL logic, decimals, date/time, Unicode, compound predicates, aggregates and LIMIT |
| `primary_key_merge`, `primary_key_sequence` | Update/delete, reinsert, repeated updates, NULL replacement across Parquet/ORC/Avro, and predicates on obsolete values |
| `partition_pruning` | Equality, IN and NULL partition filters, with EXPLAIN partition counts |
| `schema_versions` | Mixed-schema files after rename, widening, drop/add; projection and filters |
| `nested_types_*`, `map_size_native` | ARRAY/MAP/ROW, nested ARRAY, empty/NULL values, subfield projection and map-size pruning |
| `empty_table_*`, `empty_after_delete` | No-snapshot empty tables and all-deleted tables |
| `writer_snapshot` | Final rows from an independent Paimon writer update/insert |
| `predicate_pushdown` | Actual TRACE predicates, file pruning, typed literals, unsupported/partially supported predicates and final rows |
| `file_formats` | Parquet/ORC/Avro append-only results and reader routes |
| `parquet_compaction`, `orc_compaction` | Two L0 files versus one compacted file; final rows, predicates, aggregates and AUTO route |
| `statistics_append_only`, `statistics_primary_key` | Planner metadata source, row count, min/max, NULL fraction and NDV; missing-statistics fallback |
| `reader_fallback`, `reader_compatibility` | FILE/VARIANT routing and ordinary-table compatibility fallback, with reader reasons, final rows, and explicit NATIVE rejection |
| `fallback_branches`, `mixed_readers` | Real branch snapshots, main-only scans, schema mismatch rejection, and mixed raw/native execution without missing or duplicate rows |
| `partial_update_groups` | Independent/composite sequence groups, projection excluding sequence columns, field-level retract, whole-row removal, and ignored delete |
| `merge_first_row`, `merge_partial_update`, `merge_aggregation` | Parquet/ORC/Avro merge semantics, NULLs, aggregates and routing; deduplicate cases are in `primary_key_merge` |
| `global_index` | Indexed-only, unindexed-only, and mixed matches under FULL search, with explicit INDEXED_SPLIT reason assertions; forced JNI stays on Java |
| `system_table_routing` | Snapshot/schema system tables use JNI in AUTO/JNI; explicit NATIVE reports the unsupported split protocol |
| `deletion_vectors` | Parquet/ORC/Avro × NATIVE/JNI/AUTO; deleted and superseded rows excluded from results, predicates and aggregates |

Case names have the `test_paimon_` prefix. Scalar, partition, evolution, empty,
nested and primary-key cases cover NATIVE, JNI and AUTO. Mode-specific files let
other readers run even when one reader fails. Expected results describe correct
behavior; errors are not accepted as golden results.

The merge-engine and deletion-vector matrices use one case per format. AUTO runs
the full result checks; explicit NATIVE and JNI retain representative result and
routing checks. Compatible raw-file reads use the StarRocks file reader under both AUTO and
NATIVE. Cases requiring the Paimon SDK also check native execution through the BE
`PaimonNativeReader` profile. Global-index and system-table mode checks share one
case each.

AUTO prefers compatible StarRocks raw-file reads, then compatible paimon-cpp reads,
and otherwise selects JNI. Explicit JNI, including `paimon_force_jni_reader=true`,
is preserved for all split types. Explicit NATIVE first permits compatible SR
raw-file reads, then applies the same native compatibility checks as AUTO. It
reports unsupported reads instead of switching to JNI. AUTO uses JNI for metadata system-table splits and native for
compatible global-index splits.
See the [reader routing README](../../../fe/fe-core/src/main/java/com/starrocks/connector/paimon/README.md)
for the verified versions, compatibility boundaries and fallback reasons.

The merge fixtures distinguish full-row replacement from partial updates using
NULL inputs. Aggregation covers `sum` and `max`; first-row covers the compacted
result. The boundary cases extend this with `product` fallback, independent/composite
sequence groups, and representative delete policies; they do not cover every option combination. The global-index fixture checks FULL mode includes rows
written after index creation; these cases do not build or refresh indexes at runtime.

Run the reader assertion helper tests from `test/` with
`python3 -m unittest lib.test_paimon_fixture -v`.

All table mutations happen during offline data preparation, not during these cases.
In particular, schema evolution and writer-snapshot cases read already prepared
files; they do not test live cache invalidation or schema refresh. This suite does
not require a writer JDK/classpath or a Spark/Flink environment.

The additional tables were generated and verified with the Paimon 2.0.0
Java API. The existing five imported tables have unknown original generator
versions; `pk_merge_v1` has its
[original generator](https://github.com/StarRocks/starrocks-testing-data/blob/4bce0eac1a87b23982561b7c53636d125e13abf2/catalog/paimon/datagen/README.md).

## Statistics and layout assertions

The statistics fixtures contain explicit Paimon statistics computed from their
known final rows. These cases verify StarRocks consumes those values correctly;
they do not validate a Spark/Flink statistics collection job. The primary-key
fixtures have three live rows, while unmerged files contain six physical records.
Compaction fixtures retain the same logical rows with one three-record file.

Statistics cases enable `enable_paimon_column_statistics` and disable
`disable_table_stats_from_metadata_for_single_table` so EXPLAIN uses the metadata.
Missing column statistics are checked as unknown, not treated as exact values.
Predicate TRACE checks use the FE Paimon plan filters and actual file-pruning
counts; they do not by themselves prove BE row-group-level filter execution.

Reader compatibility tests follow the shared native compatibility funnel: explicit
NATIVE fails when those checks reject the input, including conservative timestamp
or MAP-key checks. It does not bypass those checks. Paimon 2.0 rejects differing
main/fallback logical row types before reader selection; that case expects the
schema error in AUTO, NATIVE, and JNI rather than successful JNI reads.

The legacy ORC timestamp case checks fractional microseconds and NULLs rather than
complete timestamp strings: legacy decoding depends on the reader JVM default time
zone, which is not fixed by the SQL session time zone. Routing assertions still
require `ORC_LEGACY_TIMESTAMP` and explicit NATIVE rejection.
