# Shared-data replication light schema change tests

`lake_replication_enable_light_schema_change` is disabled by default. With the option
unset, live rowsets referencing historical source schemas are rejected before file copy.
With it enabled, compatible scalar value-column additions and deletions are normalized
to the target current schema; this includes old and new rowsets in one snapshot.

## Focused tests

From a matching shared-data development environment, run from the repository root:

```bash
BUILD_TYPE=Debug ./run-be-ut.sh --enable-shared-data --without-java-ext --without-tenann -j 2 \
  --gtest_filter 'ReplicationSchemaUtilsTest.*:SegmentStreamConverterTest.*:LakeReplicationRemoteStorageTest.*:TxnLogApplierLakeReplicationTest.*'
```

The suites cover:

- Fresh targets, multiple historical schemas, identity and differing column UIDs,
  drop/re-add under the same name, defaults/NULL, and explicit retained/dropped UIDs.
- Missing or contradictory schemas, type/key/sort/default/generated or nested-UID
  changes, unknown schema fields, external indexes, bundle offsets, shared/range
  metadata, and invalid footer UIDs.
- Admission disabled/enabled, rejection before copy, DCG UID identity, and forced
  rewriting even when a segment UUID already exists at the target.
- Actual SegmentWriter data converted and read with the target schema: old rows use
  the re-added column default, while new rows retain their stored values. A second
  lightweight ADD followed by another replication preserves both defaults and NULLs.
- DUPLICATE KEY and PRIMARY KEY publish, durable marker round-trip, target-schema
  changes rejected before mutation, and collection of replaced files with reused
  rowset IDs.

A standalone schema-utility test is not a replacement for the full shared-data test
runtime or for cross-cluster integration tests.

## Isolated integration gate

Use disposable clusters, record exact binary revisions, and ensure all target CNs
understand the normalization marker before enabling the option. Preserve at least
one historical and one current live source rowset; compacting away all historical
rowsets does not exercise this feature.

1. Insert rows, drop/re-add an ordinary value column with a default, add a nullable
   column, and insert new rows. Create a fresh target from the current logical DDL.
2. Confirm admission fails with the option disabled, and verify that it does not
   create a successful replication log or publish partial data.
3. Enable the option and verify exact keys, values, defaults, NULLs, row counts and
   partitions, not just equality with another migration path.
4. Repeat for PRIMARY KEY (updates/deletes and persistent index on/off), aggregate
   models, multiple partitions, differing target UIDs and stale target schema maps.
5. Read again after cold restart and target compaction, then add a column and load
   more rows. Retry the same and subsequent snapshots and interrupted transfers.
6. Disable admission after preparing a marked transaction: upgraded publishers must
   still interpret the marker. A target schema change before publish must fail
   without mutating metadata. Verify cleanup never deletes live files.

The normalized transaction-log marker uses a new optional protobuf field. Old CNs
ignore that field and cannot safely replay these logs. Disabling admission alone
does not make downgrade safe; pending normalized operations must be completed or
aborted first, and published metadata must be assessed separately for rollback.
