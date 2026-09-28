# Paimon fixtures

The filesystem cases stage immutable tables from `data/` to the runner's OSS bucket.
Community and enterprise CI use the same fixtures and cases with their own `sr.conf` credentials.

## Run

From `test/`, run `python3 run.py -d sql/test_paimon_catalog -v`.
The runner needs `ossutil64` with credentials for its bucket, and the BE needs paimon-cpp.
`oss_endpoint` is used by both ossutil and the catalog. Use the internal endpoint for local development.
Override `paimon_fixture_prefix` in a local config to your own prefix, such as `joobin/paimon-fixtures`.
Do not commit credentials or local configuration. `paimon_fixture_source=repo` is the only implemented source.

## Cases

Declare a literal comma-separated list of logical table names with `paimon_stage`.
Create a filesystem catalog with `create_paimon_catalog` and put `paimon_cleanup` in a `CLEANUP` block.
Each case gets its own `${uuid0}` warehouse, including on concurrent runs.
Cleanup is idempotent and removes partial uploads even when catalog creation fails.
An externally killed runner still needs CI bucket lifecycle cleanup.

`test_paimon_reader_modes` compares scalar values against fixed expected results for JNI, NATIVE,
and AUTO, and checks the existing FE reader trace counters. AUTO on this append-only scalar fixture
uses StarRocks' Parquet reader. The helper calls this path `starrocks`; `native` means paimon-cpp.
The test also checks the native query profile, so FE routing alone cannot hide a BE fallback.
VARIANT and BLOB coverage remains in the filesystem case; those types cannot universally run through JNI.

`test_paimon_primary_key_merge` reads `pk_merge_v1` after two commits containing an update,
a delete and an insert. Two overlapping level-0 files remain unmerged, so each reader must
apply primary-key merge semantics. JNI, NATIVE and AUTO check the same fixed rows, aggregates,
and predicates on deleted keys and old values. AUTO must use JNI for this layout. The native
profile assertion also covers this merge path. See [datagen](data/datagen/README.md) for the
writer version, generation command and independent Paimon reader/layout checks.

The SQL-Tester CLEANUP UUID regression tests run without a cluster, using the regular
SQL-Tester Python dependencies. From `test/`, run
`python3 -m unittest lib.test_sql_case_cleanup -v`. They exercise the real parser and runner
through successful execution, assertion failure, and failure of an individual cleanup command.

## Data changes

Run `python3 build-support/check_paimon_fixture.py --base upstream/main` from the repository root.
The checker verifies checksums, file lists, table sizes, total size, new blob size, retired names,
and the fixture names declared by cases. It does not parse arbitrary SQL references.
The comparison uses the merge base, and includes uncommitted files so a diff can be reviewed before committing.

Tables cannot change in place. Add a new table name, migrate the cases, delete the old table, and
record its name in `retired`. Table names are never reused. Budget changes require a reason in `budget.note`.
Keep the data, manifest, generation script, and cases together in the same change.
New fixtures should include their generator and writer version, and validate the intended file layout
and expected rows independently of StarRocks' record output.

The five initial tables were imported unchanged from existing repository fixtures. Their generators
and writer versions were not present in the repository; the manifest records this provenance explicitly.
The migration retains the `paimon_test` database name and preserves every binary byte.
The new primary-key fixture records its generator, writer version, layout and expected rows.
Remote fixture repositories and enterprise-specific manifest extensions are not implemented.
