# Paimon catalog tests

From `test/`, run `python3 run.py -d sql/test_paimon_catalog -v`.
Configure the shared OSS bucket and credentials in `conf/sr.conf`. The BE needs
paimon-cpp.

## Test data

These cases read fixed tables from
`oss://${oss_bucket}/catalog/paimon/paimon_test.db/`:
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

The persistent warehouse is `oss://${oss_bucket}/catalog/paimon/`. Upload the
complete `paimon_test.db` directory before running these cases, preserving all
schema, snapshot, manifest and data files:

```text
catalog/paimon/
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
[ORDER]select id, c_int, c_smallint from paimon_readers_${uuid0}.paimon_test.scalar_types order by id;
function: assert_paimon_reader("select id, c_int, c_smallint from paimon_readers_${uuid0}.paimon_test.scalar_types order by id", "scalar_types", "native")
function: assert_paimon_native_profile("select id, c_int, c_smallint from paimon_readers_${uuid0}.paimon_test.scalar_types order by id")
```

- `assert_paimon_reader(query, table, expected)` checks FE routing with
  `TRACE VALUES EXTERNAL`. Use the table name without the catalog/database prefix.
  The expected reader must have a positive counter; the other readers must have
  zero counters. Missing counters or mixed routes fail the assertion.
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
| `reader_fallback` | AUTO selection for FILE/VARIANT, forced JNI rejection, and projection excluding unsupported columns |
| `merge_first_row`, `merge_partial_update`, `merge_aggregation` | Parquet/ORC/Avro merge semantics, NULLs, aggregates and routing; deduplicate cases are in `primary_key_merge` |
| `global_index` | Indexed and unindexed matches under FULL search; IndexedSplit forces native even when JNI is requested |
| `system_table_routing` | Snapshot/schema system tables use JNI even when NATIVE is requested |
| `deletion_vectors` | Parquet/ORC/Avro × NATIVE/JNI/AUTO; deleted and superseded rows excluded from results, predicates and aggregates |

Case names have the `test_paimon_` prefix. Scalar, partition, evolution, empty,
nested and primary-key cases cover NATIVE, JNI and AUTO. Mode-specific files let
other readers run even when one reader fails. Expected results describe correct
behavior; errors are not accepted as golden results.

The merge-engine and deletion-vector matrices use one case per format. AUTO runs
the full result checks; explicit NATIVE and JNI retain representative result and
routing checks. Native format, first-row and DV checks remain broader where AUTO
uses the StarRocks file reader. Native execution is also checked through the BE
`PaimonNativeReader` profile. Global-index and system-table mode checks share one
case each.

AUTO keeps the StarRocks raw-file fast path and uses paimon-cpp for other ordinary
data splits. System tables remain on JNI; global-index splits remain native even
when JNI is requested. Explicit JNI is preserved for ordinary data splits.

The merge fixtures distinguish full-row replacement from partial updates using
NULL inputs. Aggregation covers `sum` and `max`; first-row covers the compacted
result. They do not cover every aggregation function, partial-update sequence
group, or delete policy. The global-index fixture checks FULL mode includes rows
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
