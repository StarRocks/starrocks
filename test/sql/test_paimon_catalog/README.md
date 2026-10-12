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
may need manual cleanup. Follow `T/test_paimon_reader_modes` for a single-table example.

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
