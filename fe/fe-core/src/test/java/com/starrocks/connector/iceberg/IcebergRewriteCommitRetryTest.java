// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

// A rewrite (rewrite_data_files) racing an external writer that appends to the same table.
//
// The external writer is simulated by an append committed through a separate table handle right before
// the rewrite's catalog commit, so every such attempt loses the optimistic metadata swap -- the shape of a
// streaming writer whose commit interval is shorter than one rewrite commit attempt.
public class IcebergRewriteCommitRetryTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    private static final Map<String, String> CONFIG = new HashMap<>();
    // 1 first try + commit.retry.num-retries retries, per Iceberg transaction commit.
    private static final int DEFAULT_ATTEMPTS_PER_TXN = 1 + TableProperties.COMMIT_NUM_RETRIES_DEFAULT;

    static {
        CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732"); // non-exist ip
        CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDefaultCtx();
    }

    // Table operations of the rewrite's table handle: before each of the first `preemptions` catalog
    // commits, an external writer commits first (by default, appends a new file to another partition).
    private class PreemptedOps extends TestTables.TestTableOperations {
        private final Table externalWriter;
        private final BiConsumer<Table, Integer> externalCommit;
        private int preemptions;
        private int attempts = 0;
        private int externalAppends = 0;

        PreemptedOps(int preemptions) {
            this(preemptions, (writer, i) -> writer.newAppend().appendFile(externalFile(i)).commit());
        }

        PreemptedOps(int preemptions, BiConsumer<Table, Integer> externalCommit) {
            super("ta", tableDir);
            this.preemptions = preemptions;
            this.externalCommit = externalCommit;
            // A separate handle with its own snapshot id range, like an independent writer.
            this.externalWriter = new BaseTable(new TestTables.TestTableOperations("ta", tableDir) {
                private long nextId = 1_000_000L;

                @Override
                public long newSnapshotId() {
                    return ++nextId;
                }
            }, "ta");
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updatedMetadata) {
            attempts++;
            if (preemptions > 0) {
                preemptions--;
                externalAppends++;
                externalWriter.refresh();
                externalCommit.accept(externalWriter, externalAppends);
            }
            super.commit(base, updatedMetadata);
        }
    }

    private static DataFile externalFile(int i) {
        return DataFiles.builder(SPEC_A)
                .withPath("/path/to/external-append-" + i + ".parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("data_bucket=1")
                .withRecordCount(1)
                .build();
    }

    private IcebergMetadata newMetadata(Table nativeTable) {
        IcebergHiveCatalog catalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, catalog,
                Executors.newSingleThreadExecutor(), new IcebergCatalogProperties(CONFIG));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), nativeTable, Maps.newHashMap());
        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };
        return metadata;
    }

    private TIcebergDataFile rewriteOutput() {
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setColumn_sizes(Map.of());
        stats.setValue_counts(Map.of());
        stats.setNull_value_counts(Map.of());
        stats.setLower_bounds(Map.of());
        stats.setUpper_bounds(Map.of());
        TIcebergDataFile dataFile = new TIcebergDataFile();
        dataFile.setPath(mockedNativeTableA.location() + "/data/data_bucket=0/rewritten.parquet");
        dataFile.setFormat("parquet");
        dataFile.setRecord_count(2);
        dataFile.setSplit_offsets(Lists.newArrayList(4L));
        dataFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=0/");
        dataFile.setFile_size_in_bytes(512);
        dataFile.setPartition_null_fingerprint("0");
        dataFile.setColumn_stats(stats);
        return dataFile;
    }

    // Plans the rewrite of FILE_A against the current snapshot and commits it through `ops`.
    private void runRewrite(PreemptedOps ops, long baseSnapshotId) {
        runRewrite(ops, baseSnapshotId, file -> { });
    }

    private void runRewrite(PreemptedOps ops, long baseSnapshotId, Consumer<TIcebergDataFile> customizeOutput) {
        IcebergMetadata metadata = newMetadata(new BaseTable(ops, "ta"));
        TSinkCommitInfo commitInfo = new TSinkCommitInfo();
        commitInfo.setIs_rewrite(true);
        TIcebergDataFile output = rewriteOutput();
        customizeOutput.accept(output);
        commitInfo.setIceberg_data_file(output);
        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.addScannedDataFiles(Sets.newHashSet(FILE_A));
        extra.setBaseSnapshotId(baseSnapshotId);
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(commitInfo), null, extra);
    }

    private Set<String> liveDataFiles() throws IOException {
        mockedNativeTableA.refresh();
        Set<String> paths = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = mockedNativeTableA.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().location());
            }
        }
        return paths;
    }

    private void assertRewriteCommitted(int externalAppends) throws IOException {
        mockedNativeTableA.refresh();
        Snapshot current = mockedNativeTableA.currentSnapshot();
        Assertions.assertEquals(DataOperations.REPLACE, current.operation());
        Set<String> live = liveDataFiles();
        Assertions.assertFalse(live.contains(FILE_A.location()), "the rewritten file must be replaced");
        Assertions.assertTrue(live.contains(mockedNativeTableA.location() + "/data/data_bucket=0/rewritten.parquet"));
        for (int i = 1; i <= externalAppends; i++) {
            Assertions.assertTrue(live.contains(externalFile(i).location()),
                    "the external append " + i + " must survive the rewrite");
        }
    }

    // The rewrite is rebased onto external appends: Iceberg's transaction commit refreshes and re-applies it.
    @Test
    public void testRewriteRebasesOntoExternalAppendsWithinIcebergRetries() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long base = mockedNativeTableA.currentSnapshot().snapshotId();
        PreemptedOps ops = new PreemptedOps(DEFAULT_ATTEMPTS_PER_TXN - 1);

        runRewrite(ops, base);

        Assertions.assertEquals(DEFAULT_ATTEMPTS_PER_TXN, ops.attempts);
        assertRewriteCommitted(DEFAULT_ATTEMPTS_PER_TXN - 1);
    }

    // Issue #76955: an external writer that wins every attempt of Iceberg's default retry budget fails the
    // rewrite, although nothing it does conflicts with the rewrite. This is the behavior without the extra
    // StarRocks-level retries.
    @Test
    public void testRewriteFailsWhenExternalAppendWinsEveryDefaultAttempt() throws Exception {
        int saved = Config.iceberg_rewrite_commit_max_retries;
        Config.iceberg_rewrite_commit_max_retries = 0;
        try {
            mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
            long base = mockedNativeTableA.currentSnapshot().snapshotId();
            PreemptedOps ops = new PreemptedOps(DEFAULT_ATTEMPTS_PER_TXN);

            StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> runRewrite(ops, base));

            Assertions.assertTrue(e.getMessage().contains("table was updated"), e.getMessage());
            Assertions.assertEquals(DEFAULT_ATTEMPTS_PER_TXN, ops.attempts);
            Assertions.assertTrue(liveDataFiles().contains(FILE_A.location()));
        } finally {
            Config.iceberg_rewrite_commit_max_retries = saved;
        }
    }

    // The same contention commits once the budget is one attempt larger: the failure is the retry budget.
    @Test
    public void testLargerTableRetryBudgetCommitsUnderSameContention() throws Exception {
        int saved = Config.iceberg_rewrite_commit_max_retries;
        Config.iceberg_rewrite_commit_max_retries = 0;
        try {
            mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
            mockedNativeTableA.updateProperties()
                    .set(TableProperties.COMMIT_NUM_RETRIES, String.valueOf(DEFAULT_ATTEMPTS_PER_TXN))
                    .commit();
            long base = mockedNativeTableA.currentSnapshot().snapshotId();
            PreemptedOps ops = new PreemptedOps(DEFAULT_ATTEMPTS_PER_TXN);

            runRewrite(ops, base);

            Assertions.assertEquals(DEFAULT_ATTEMPTS_PER_TXN + 1, ops.attempts);
            assertRewriteCommitted(DEFAULT_ATTEMPTS_PER_TXN);
        } finally {
            Config.iceberg_rewrite_commit_max_retries = saved;
        }
    }

    // With the default config, the rewrite retries on the latest table once Iceberg's retries are exhausted.
    @Test
    public void testRewriteRetriesAfterIcebergRetriesAreExhausted() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long base = mockedNativeTableA.currentSnapshot().snapshotId();
        PreemptedOps ops = new PreemptedOps(DEFAULT_ATTEMPTS_PER_TXN);

        runRewrite(ops, base);

        Assertions.assertEquals(DEFAULT_ATTEMPTS_PER_TXN + 1, ops.attempts);
        assertRewriteCommitted(DEFAULT_ATTEMPTS_PER_TXN);
    }

    // The extra retries are bounded: a writer that wins forever still fails the rewrite.
    @Test
    public void testRewriteRetriesAreBounded() throws Exception {
        int saved = Config.iceberg_rewrite_commit_max_retries;
        Config.iceberg_rewrite_commit_max_retries = 1;
        try {
            mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
            long base = mockedNativeTableA.currentSnapshot().snapshotId();
            PreemptedOps ops = new PreemptedOps(Integer.MAX_VALUE);

            StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> runRewrite(ops, base));

            Assertions.assertTrue(e.getMessage().contains("table was updated"), e.getMessage());
            Assertions.assertEquals(2 * DEFAULT_ATTEMPTS_PER_TXN, ops.attempts);
            Assertions.assertTrue(liveDataFiles().contains(FILE_A.location()));
        } finally {
            Config.iceberg_rewrite_commit_max_retries = saved;
        }
    }

    // A concurrent delete on a file being rewritten is a conflict, not a lost race: it is not retried.
    @Test
    public void testRewriteConflictIsNotRetried() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long base = mockedNativeTableA.currentSnapshot().snapshotId();
        PreemptedOps ops = new PreemptedOps(1,
                (writer, i) -> writer.newRowDelta().addDeletes(FILE_A_DELETES).commit());

        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> runRewrite(ops, base));

        Assertions.assertTrue(e.getMessage().contains("found new delete for replaced data file"), e.getMessage());
        Assertions.assertEquals(1, ops.attempts);
        Assertions.assertTrue(liveDataFiles().contains(FILE_A.location()));
    }

    private DataFile liveRewrittenFile() throws IOException {
        mockedNativeTableA.refresh();
        String path = mockedNativeTableA.location() + "/data/data_bucket=0/rewritten.parquet";
        try (CloseableIterable<FileScanTask> tasks = mockedNativeTableA.newScan().includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().location().equals(path)) {
                    return task.file();
                }
            }
        }
        throw new AssertionError("the rewritten file is not live");
    }

    // A retried rewrite commits the bounds of a DECIMAL64 column as the BE reported them: the BE's
    // little-endian Parquet bound is converted to Iceberg's big-endian form once, not once per retry.
    @Test
    public void testRetriedRewriteKeepsDecimalBounds() throws Exception {
        mockedNativeTableA.updateSchema().addColumn("dec", Types.DecimalType.of(10, 2)).commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long base = mockedNativeTableA.currentSnapshot().snapshotId();
        int decId = mockedNativeTableA.schema().findField("dec").fieldId();
        PreemptedOps ops = new PreemptedOps(DEFAULT_ATTEMPTS_PER_TXN);

        runRewrite(ops, base, output -> {
            Map<Integer, ByteBuffer> lower = new HashMap<>();
            Map<Integer, ByteBuffer> upper = new HashMap<>();
            lower.put(decId, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, 12345L));
            upper.put(decId, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, 67890L));
            output.getColumn_stats().setLower_bounds(lower);
            output.getColumn_stats().setUpper_bounds(upper);
        });

        Assertions.assertEquals(DEFAULT_ATTEMPTS_PER_TXN + 1, ops.attempts);
        assertRewriteCommitted(DEFAULT_ATTEMPTS_PER_TXN);
        DataFile rewritten = liveRewrittenFile();
        Types.DecimalType decType = Types.DecimalType.of(10, 2);
        Assertions.assertEquals(new BigDecimal("123.45"),
                Conversions.fromByteBuffer(decType, rewritten.lowerBounds().get(decId)));
        Assertions.assertEquals(new BigDecimal("678.90"),
                Conversions.fromByteBuffer(decType, rewritten.upperBounds().get(decId)));
    }

    // The partition spec evolves while the rewrite waits to retry: the rewritten file still lands under the
    // spec the BE wrote it with, not the spec current at the retry.
    @Test
    public void testRetriedRewriteKeepsWriteSpecAcrossSpecEvolution() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long base = mockedNativeTableA.currentSnapshot().snapshotId();
        int writeSpecId = mockedNativeTableA.spec().specId();
        PreemptedOps ops = new PreemptedOps(DEFAULT_ATTEMPTS_PER_TXN, (writer, i) -> {
            if (i < DEFAULT_ATTEMPTS_PER_TXN) {
                writer.newAppend().appendFile(externalFile(i)).commit();
            } else {
                writer.updateSpec().addField("id").commit();
            }
        });

        runRewrite(ops, base);

        Assertions.assertEquals(DEFAULT_ATTEMPTS_PER_TXN + 1, ops.attempts);
        assertRewriteCommitted(DEFAULT_ATTEMPTS_PER_TXN - 1);
        Assertions.assertNotEquals(writeSpecId, mockedNativeTableA.spec().specId(), "the spec must have evolved");
        Assertions.assertEquals(writeSpecId, liveRewrittenFile().specId());
    }
}
