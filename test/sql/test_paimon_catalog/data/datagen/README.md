# Primary-key merge fixture

`GeneratePrimaryKeyFixture.java` uses the Apache Paimon **2.0.0** Java API directly.
Generation requires a JDK and the Paimon bundle plus its runtime dependencies; Spark,
Flink, a metastore and object storage are not needed. Generation is a maintainer task;
SQL tests consume the checked-in files without running Java datagen.

Use a classpath containing only Paimon 2.0.0, for example a freshly packaged StarRocks
FE `lib/*` directory. Old jars left by incremental packaging must not be mixed in.
From the repository root, with `PAIMON_FIXTURE_CLASSPATH` set to that classpath:

```bash
fixture_work=$(mktemp -d ./paimon-fixture.XXXXXX)
javac -proc:none -cp "$PAIMON_FIXTURE_CLASSPATH" -d "$fixture_work" \
  test/sql/test_paimon_catalog/data/datagen/GeneratePrimaryKeyFixture.java
java -Xmx256m -cp "$PAIMON_FIXTURE_CLASSPATH:$fixture_work" \
  GeneratePrimaryKeyFixture generate "$fixture_work/warehouse"

# Independently verify the checked-in fixture, including after relocating it.
java -Xmx256m -cp "$PAIMON_FIXTURE_CLASSPATH:$fixture_work" \
  GeneratePrimaryKeyFixture verify test/sql/test_paimon_catalog/data
```

The generator refuses an existing output warehouse. It produces `paimon_test.db/pk_merge_v1`
using one bucket, Parquet, the deduplicate merge engine and `write-only=true` to prevent compaction.

| Commit | Changes |
| --- | --- |
| 1 | Insert `(1, 10, old)`, `(2, 20, deleted)`, `(3, 30, kept)` |
| 2 | Update key 1 to `(1, 100, updated)`, delete key 2, insert `(4, 40, added)` |

The generator verifies two snapshots, two level-0 files, six physical records including
one delete record, and a split that cannot be converted into a raw file scan. It also
reads the table with Paimon's own reader and asserts the final rows:

```text
1,100,updated
3,30,kept
4,40,added
```

These rows define the SQL expectations: count 3, sum 170, no key 2, no old amount 10,
and keys 1 and 4 for `amount >= 40`. They are not obtained by recording StarRocks output.
The SQL case checks JNI, NATIVE and AUTO; AUTO must choose JNI for this merge split.

File names and timestamps vary across generation runs. Reproduction guarantees the
logical rows and merge layout, not identical bytes. The checked-in fixture is immutable:
future changes need a new table name and corresponding manifest entry; do not overwrite
`pk_merge_v1` with a regenerated copy.
