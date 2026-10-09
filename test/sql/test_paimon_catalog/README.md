# Paimon fixtures

The filesystem cases stage immutable tables from `data/` to the runner's OSS bucket.
Community and enterprise CI use the same fixtures and cases with their own `sr.conf` credentials.

## Run

From `test/`, run `python3 run.py -d sql/test_paimon_catalog -v`.
The runner needs `ossutil64` with credentials for its bucket, and the BE needs paimon-cpp.
`oss_endpoint` is used by both ossutil and the catalog. Use the internal endpoint for local development.
The default `paimon_fixture_prefix` is `paimon_ci_test`, including when absent from older configs.
Override it in a local config to your own prefix, such as `my-tests/paimon-fixtures`.
Do not commit credentials or local configuration. Fixtures are loaded from this repository's `data/`
directory, located relative to the helper module rather than the working directory.
No additional file inventory, checksum or size checks are performed. Keep fixture files unchanged during a test run.

## Cases

Declare the OSS dependency explicitly with `paimon_stage("${oss_bucket}", "${uuid0}", "database.table")`.
Table lists accept comma-separated names with optional surrounding whitespace.
Create a filesystem catalog with `create_paimon_catalog`.
Each case gets its own `${uuid0}` warehouse, including on concurrent runs.

Call `paimon_cleanup()` as the final normal statement so cleanup failures fail a successful case.
Also call it in the existing `CLEANUP` block to attempt cleanup after a query or upload failure.
The helper saves its resolved warehouse and catalog on the case instance; the CLEANUP block needs
no UUID substitution. Successful cleanup clears the saved target, so the second call does nothing.
Failed cleanup retains the target for a retry. Partial uploads are removed even if no catalog was created.
The existing framework logs failures from the fallback CLEANUP block; this PR does not change that behavior.
An externally killed runner still needs external cleanup, such as an OSS lifecycle policy.

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

## Helper tests and data maintenance

From `test/`, run `python3 -m unittest lib.test_paimon_fixture -v` for the Paimon helpers.
The Paimon helper methods are defined in `test/lib/sr_sql_lib.py`.
The tests cover upload failures, cleanup retries, reader assertions and the existing runner lifecycle.

The five initial tables were imported unchanged from existing repository fixtures. Their generators
and writer versions were not recorded. The migration retains the `paimon_test` database name and
preserves every binary byte. The primary-key fixture includes its generator, writer version and
independent assertions for the file layout and expected rows.

Keep fixture datasets small. Add new scenarios under new table names and include the generator
and expected results where possible. These maintenance guidelines are reviewed manually.
