# Paimon catalog tests

Initialize the test data from the StarRocks repository root:

```bash
git submodule update --init test/data
```

From `test/`, run `python3 run.py -d sql/test_paimon_catalog -v`.
Configure OSS access in `conf/sr.conf` and ossutil64. The BE needs paimon-cpp.

## Data preparation

Cases use `shell: ossutil64 cp -r` to upload tables from
`./data/catalog/paimon/warehouse/paimon_test.db/` to
`oss://${oss_bucket}/test_catalog/<case-name>/${uuid0}/paimon_test.db/`.
Each case creates its filesystem catalog with SQL and removes the catalog and
uploaded files at the end. If a case stops early, its UUID directory and catalog
may need manual cleanup. Follow [T/test_paimon_reader_modes](T/test_paimon_reader_modes)
for a complete single-table example, including upload, catalog creation and cleanup.

## Reader assertions

After uploading `scalar_types` and creating the catalog as in that example, add
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
`starrocks` for `scalar_types`, or `jni` for `pk_merge_v1`. There is no `auto`
assertion value. Use a query that scans actual data; an empty scan may have no
positive reader counter or reader profile section.

Each helper runs an additional query and returns `None` on success; its R-file
result block therefore contains `None`. Keep the ordinary SELECT and its expected
rows to verify results as well as the reader path.

## Coverage

- `test_paimon_filesystem_catalog`: existing scalar, NULL, VARIANT and BLOB checks.
- `test_paimon_reader_modes`: scalar results in NATIVE, JNI and AUTO modes;
  AUTO uses the StarRocks Parquet reader for this fixture.
- `test_paimon_primary_key_merge`: updates, deletes and inserts across overlapping
  L0 files in all three modes; AUTO uses JNI for this layout.

Reader assertions check existing FE EXTERNAL trace counters. NATIVE cases also
check the BE `PaimonNativeReader` profile. Run their helper tests from `test/` with
`python3 -m unittest lib.test_paimon_fixture -v`.

See [datagen](../../data/catalog/paimon/datagen/README.md) for primary-key fixture
generation and independent verification. The other five tables retain the existing
fixture contents; their original generator versions are unknown.
