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

package com.starrocks.connector.fluss;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FlussTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.metadata.AcquireKvSnapshotLeaseResult;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.utils.LakeSourceUtils;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class FlussMetadataTest {
    private static final String CATALOG = "fluss0";
    private static final String DB = "db1";
    private static final String TABLE = "orders";
    private static final long TABLE_ID = 42L;

    @Mocked
    private Admin admin;

    @Mocked
    private KvSnapshotLease kvSnapshotLease;

    private FlussMetadata metadata;

    @BeforeEach
    public void setUp() {
        // Split planning runs within a query.
        ConnectContext context = new ConnectContext();
        context.setQueryId(UUIDUtil.genUUID());
        context.setThreadLocalInfo();
        Configuration catalogConf = new Configuration();
        catalogConf.setString("bootstrap.servers", "localhost:9123");
        metadata = new FlussMetadata(CATALOG, new HdfsEnvironment(), admin, catalogConf);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testGetDb() {
        new Expectations() {
            {
                admin.getDatabaseInfo(DB);
                result = CompletableFuture.completedFuture(null);
                admin.getDatabaseInfo("missing");
                result = CompletableFuture.failedFuture(
                        new DatabaseNotExistException("database does not exist"));
            }
        };

        Assertions.assertEquals(DB, metadata.getDb(new ConnectContext(), DB).getFullName());
        Assertions.assertNull(metadata.getDb(new ConnectContext(), "missing"));
    }

    @Test
    public void testGetTable() {
        TableInfo tableInfo = tableInfo(true);
        new Expectations() {
            {
                admin.getTableInfo(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(tableInfo);
            }
        };

        FlussTable table = (FlussTable) metadata.getTable(new ConnectContext(), DB, TABLE);
        Assertions.assertEquals(CATALOG, table.getCatalogName());
        Assertions.assertEquals(DB, table.getCatalogDBName());
        Assertions.assertEquals(TABLE, table.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("dt", "region"), table.getPartitionColumnNames());
        Assertions.assertEquals(Lists.newArrayList("id", "name", "dt", "region"), table.getFieldNames());
        Assertions.assertEquals(IntegerType.INT, table.getBaseSchema().get(0).getType());
        Assertions.assertEquals(TypeFactory.createDefaultCatalogString(), table.getBaseSchema().get(1).getType());
        Assertions.assertEquals("test fluss table", table.getComment());
    }

    @Test
    public void testGetPrimaryKeyTableWithReadModeSuffix() {
        TableInfo tableInfo = tableInfo(false, true);
        new Expectations() {
            {
                admin.getTableInfo(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(tableInfo);
            }
        };

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getTable(new ConnectContext(), DB, TABLE + "$lake"));
    }

    @Test
    public void testGetTableWithLakeAndRt() {
        TableInfo tableInfo = tableInfo(false);
        new Expectations() {
            {
                admin.getTableInfo(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(tableInfo);
            }
        };

        FlussTable lakeTable = (FlussTable) metadata.getTable(new ConnectContext(), DB, TABLE + "$lake");
        Assertions.assertEquals(TABLE, lakeTable.getCatalogTableName());
        Assertions.assertEquals("$lake", lakeTable.getTableNameSuffix());

        FlussTable rtTable = (FlussTable) metadata.getTable(new ConnectContext(), DB, TABLE + "$rt");
        Assertions.assertEquals(TABLE, rtTable.getCatalogTableName());
        Assertions.assertEquals("$rt", rtTable.getTableNameSuffix());
        Assertions.assertNotEquals(lakeTable.getUUID(), rtTable.getUUID());
        Assertions.assertNotEquals(lakeTable.getTableIdentifier(), rtTable.getTableIdentifier());
        Assertions.assertNotEquals(lakeTable, rtTable);
    }

    @Test
    public void testTableExistsWithLakeAndRt() {
        TablePath basePath = TablePath.of(DB, TABLE);
        new Expectations() {
            {
                admin.tableExists(basePath);
                result = CompletableFuture.completedFuture(true);
                minTimes = 0;
            }
        };

        Assertions.assertTrue(metadata.tableExists(new ConnectContext(), DB, TABLE));
        Assertions.assertTrue(metadata.tableExists(new ConnectContext(), DB, TABLE + "$lake"));
        Assertions.assertTrue(metadata.tableExists(new ConnectContext(), DB, TABLE + "$rt"));
        new Verifications() {
            {
                admin.tableExists(basePath);
                times = 3;
            }
        };
    }

    @Test
    public void testListPartitionNames() {
        PartitionInfo p1 = partitionInfo(1001L, "2026-07-01", "cn");
        PartitionInfo p2 = partitionInfo(1002L, "2026-07-02", "us");
        new Expectations() {
            {
                admin.listPartitionInfos(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(Lists.newArrayList(p1, p2));
            }
        };

        List<String> partitionNames =
                metadata.listPartitionNames(DB, TABLE, ConnectorMetadataRequestContext.DEFAULT);
        Assertions.assertEquals(Lists.newArrayList("dt=2026-07-01/region=cn", "dt=2026-07-02/region=us"),
                partitionNames);

        List<com.starrocks.connector.PartitionInfo> partitions = metadata.getPartitions(
                flussTable(tableInfo(true), ""), Lists.newArrayList(partitionNames.get(0), "dt=missing/region=missing"));
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals(partitionNames.get(0), ((Partition) partitions.get(0)).getPartitionName());
    }

    @Test
    public void testUnpartitionedTablePartitions() {
        new Expectations() {
            {
                admin.listPartitionInfos(TablePath.of(DB, TABLE));
                result = CompletableFuture.<List<PartitionInfo>>failedFuture(
                        new TableNotPartitionedException("table is not partitioned"));
            }
        };

        Assertions.assertTrue(metadata.listPartitionNames(
                DB, TABLE, ConnectorMetadataRequestContext.DEFAULT).isEmpty());

        List<com.starrocks.connector.PartitionInfo> partitions = metadata.getPartitions(
                flussTable(tableInfo(false), ""), Collections.emptyList());
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals(TABLE, ((Partition) partitions.get(0)).getPartitionName());
    }

    @Test
    public void testGetRemoteFilesWithEmptyPartitionKeys() {
        FlussTable table = flussTable(tableInfo(true), "");
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPartitionKeys(Collections.emptyList())
                .build();
        List<SourceSplitBase> splits = getFlussSplits(table, params);

        Assertions.assertTrue(splits.isEmpty());
    }

    @Test
    public void testGetRemoteFilesNoLakeSnapshot() {
        mockPaimonLakeWithoutSnapshot();
        FlussTable table = flussTable(tableInfo(false), "");

        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getRemoteFiles(table, GetRemoteFilesParams.newBuilder().build()));
        Assertions.assertTrue(exception.getMessage().contains("No readable Fluss lake snapshot exists"));
    }

    @ParameterizedTest
    @CsvSource({"false, paimon, ''", "true, iceberg, ''", "true, iceberg, $lake"})
    public void testGetRemoteFilesUnsupportedTable(boolean dataLakeEnabled, String format, String suffix) {
        FlussTable table = flussTable(tableInfo(false, false, dataLakeEnabled, format), suffix);

        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getRemoteFiles(table, GetRemoteFilesParams.newBuilder().build()));
        Assertions.assertEquals(
                "Fluss table fluss0.db1.orders is not supported. StarRocks only supports reading Fluss tables with " +
                        "'table.datalake.enabled' = 'true' and 'table.datalake.format' = 'paimon'. " +
                        "Query orders$rt to read the Fluss log.",
                exception.getMessage());
    }

    @Test
    public void testRtReadOnSelectedPartitionsOnly() throws Exception {
        mockLakeSourceUnused();
        PartitionInfo p1 = partitionInfo(1001L, "2026-07-01", "cn");
        PartitionInfo p2 = partitionInfo(1002L, "2026-07-02", "us");
        // Only the selected partition is asked for offsets.
        mockLatestOffsets(Map.of(p1.getPartitionName(), Map.of(0, 2L, 1, 0L, 2, 1L)));
        new Expectations() {
            {
                admin.listPartitionInfos(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(Lists.newArrayList(p1, p2));
                admin.getReadableLakeSnapshot(TablePath.of(DB, TABLE));
                result = CompletableFuture.failedFuture(new LakeTableSnapshotNotExistException("no snapshot"));
            }
        };
        FlussTable table = flussTable(tableInfo(true, false, false, "iceberg"), "$rt");
        PartitionKey selected = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2026-07-01"), new PartitionValue("cn")),
                table.getPartitionColumns());

        List<SourceSplitBase> splits = getFlussSplits(table,
                GetRemoteFilesParams.newBuilder().setPartitionKeys(Lists.newArrayList(selected)).build());

        Assertions.assertEquals(2, splits.size());
        assertLogSplit(splits.get(0), 1001L, 0, 0L, 2L);
        assertLogSplit(splits.get(1), 1001L, 2, 0L, 1L);
    }

    @ParameterizedTest
    @CsvSource({"true, iceberg", "false, paimon", "true, paimon"})
    public void testRtReadPlansLogSplitsWithoutTheLake(boolean dataLakeEnabled, String format) {
        mockLakeSourceUnused();
        mockLatestOffsets(Map.of("", Map.of(0, 5L, 1, 0L, 2, 7L)));
        new Expectations() {
            {
                admin.getReadableLakeSnapshot(TablePath.of(DB, TABLE));
                result = CompletableFuture.failedFuture(new LakeTableSnapshotNotExistException("no snapshot"));
            }
        };
        FlussTable table = flussTable(tableInfo(false, false, dataLakeEnabled, format), "$rt");

        List<SourceSplitBase> splits = getFlussSplits(table, GetRemoteFilesParams.newBuilder().build());

        // Bucket 1 is empty, so it gets no split.
        Assertions.assertEquals(2, splits.size());
        assertLogSplit(splits.get(0), null, 0, 0L, 5L);
        assertLogSplit(splits.get(1), null, 2, 0L, 7L);
    }

    @ParameterizedTest
    @CsvSource({"true, iceberg", "false, paimon", "true, paimon"})
    public void testPrimaryKeyTableIsReadFromFlussWhenTheLakeCannotBeUnionRead(boolean dataLakeEnabled,
                                                                                String format) {
        if (dataLakeEnabled && format.equals("paimon")) {
            mockPaimonLakeWithoutSnapshot();
        } else {
            mockLakeSourceUnused();
        }
        mockPrimaryKeySnapshots();
        FlussTable table = flussTable(tableInfo(false, true, dataLakeEnabled, format), "");

        List<SourceSplitBase> splits = getFlussSplits(table, GetRemoteFilesParams.newBuilder().build());

        // Bucket 1 has no KV snapshot yet, so its whole changelog is read.
        Assertions.assertEquals(3, splits.size());
        assertSnapshotLogSplit(splits.get(0), null, 0, 5L, 10L, 12L);
        assertSnapshotLogSplit(splits.get(1), null, 1, -1L, LogScanner.EARLIEST_OFFSET, 3L);
        assertSnapshotLogSplit(splits.get(2), null, 2, 6L, 7L, 7L);
    }

    @Test
    public void testGetTableStatistics() {
        new Expectations() {
            {
                admin.getTableStats(TablePath.of(DB, TABLE));
                returns(CompletableFuture.completedFuture(new TableStats(123L)),
                        CompletableFuture.failedFuture(new RuntimeException("failed")));
            }
        };
        FlussTable table = flussTable(tableInfo(false), "");

        Statistics statistics = metadata.getTableStatistics(null, table, Collections.emptyMap(),
                Collections.emptyList(), null, -1, TvrTableSnapshot.empty());
        Assertions.assertEquals(123D, statistics.getOutputRowCount());

        statistics = metadata.getTableStatistics(null, table, Collections.emptyMap(),
                Collections.emptyList(), null, -1, TvrTableSnapshot.empty());
        Assertions.assertEquals((double) Config.default_statistics_output_row_count, statistics.getOutputRowCount());

        for (String tableNameSuffix : Arrays.asList("$lake", "$rt")) {
            table = flussTable(tableInfo(false), tableNameSuffix);
            statistics = metadata.getTableStatistics(null, table, Collections.emptyMap(),
                    Collections.emptyList(), null, -1, TvrTableSnapshot.empty());
            Assertions.assertEquals(
                    (double) Config.default_statistics_output_row_count, statistics.getOutputRowCount());
        }
    }

    @Test
    public void testPrimaryKeyReadLeasesTheKvSnapshotsItReads() {
        mockLakeSourceUnused();
        mockPrimaryKeySnapshots();
        List<Map<TableBucket, Long>> acquired = mockLease(Collections.emptyMap());
        FlussTable table = flussTable(tableInfo(false, true, true, "iceberg"), "");

        FlussSplitsInfo splitsInfo = getFlussSplitsInfo(table);

        // Bucket 1 has no KV snapshot, so only buckets 0 and 2 are leased.
        Assertions.assertEquals(List.of(Map.of(new TableBucket(TABLE_ID, 0), 5L, new TableBucket(TABLE_ID, 2), 6L)),
                acquired);
        Assertions.assertTrue(splitsInfo.getKvSnapshotLease().leaseId().isPresent());
        new Verifications() {
            {
                kvSnapshotLease.dropLease();
                times = 0;
            }
        };
    }

    @Test
    public void testKvSnapshotLeaseOutlastsTheQueryTimeoutAndQueueWait() {
        mockLakeSourceUnused();
        mockPrimaryKeySnapshots();
        mockLease(Collections.emptyMap());
        FlussTable table = flussTable(tableInfo(false, true, true, "iceberg"), "");
        int queuePendingTimeoutS = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        GlobalVariable.setQueryQueuePendingTimeoutSecond(120);
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setQueryTimeoutS(600);
        context.setQueryId(UUIDUtil.genUUID());
        context.setThreadLocalInfo();
        try {
            FlussSplitsInfo splitsInfo = getFlussSplitsInfo(table);
            // The lease id names the query, so a lease left in Fluss can be matched to it.
            Assertions.assertTrue(splitsInfo.getKvSnapshotLease().leaseId().orElseThrow()
                    .startsWith("starrocks-" + context.getQueryId() + "-"));
        } finally {
            ConnectContext.remove();
            GlobalVariable.setQueryQueuePendingTimeoutSecond(queuePendingTimeoutS);
        }
        new Verifications() {
            {
                // The query may wait up to the queue pending timeout after planning, then run for its timeout.
                admin.createKvSnapshotLease(anyString, 720_000L);
                times = 1;
            }
        };
    }

    @Test
    public void testPlainExplainTakesNoLease() {
        mockLakeSourceUnused();
        mockPrimaryKeySnapshots();
        FlussTable table = flussTable(tableInfo(false, true, true, "iceberg"), "");
        ConnectContext context = new ConnectContext();
        context.setExplainLevel(StatementBase.ExplainLevel.VERBOSE);
        context.setThreadLocalInfo();
        try {
            // Nothing runs or clears the scans of a plain EXPLAIN, so a lease would stay until it expires.
            FlussSplitsInfo splitsInfo = getFlussSplitsInfo(table);
            Assertions.assertEquals(3, splitsInfo.getFlussSplits().size());
            Assertions.assertSame(FlussKvSnapshotLease.NONE, splitsInfo.getKvSnapshotLease());
        } finally {
            ConnectContext.remove();
        }
        new Verifications() {
            {
                admin.createKvSnapshotLease(anyString, anyLong);
                times = 0;
            }
        };
    }

    @Test
    public void testExplainAnalyzeLeasesBecauseItRunsTheQuery() {
        mockLakeSourceUnused();
        mockPrimaryKeySnapshots();
        mockLease(Collections.emptyMap());
        FlussTable table = flussTable(tableInfo(false, true, true, "iceberg"), "");
        ConnectContext context = new ConnectContext();
        context.setExplainLevel(StatementBase.ExplainLevel.ANALYZE);
        context.setThreadLocalInfo();
        try {
            Assertions.assertTrue(getFlussSplitsInfo(table).getKvSnapshotLease().leaseId().isPresent());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testPrimaryKeyReadFailsAndDropsTheLeaseWhenASnapshotIsGone() {
        mockLakeSourceUnused();
        mockPrimaryKeySnapshots();
        mockLease(Map.of(new TableBucket(TABLE_ID, 2), 6L));
        FlussTable table = flussTable(tableInfo(false, true, true, "iceberg"), "");

        StarRocksConnectorException error =
                Assertions.assertThrows(StarRocksConnectorException.class, () -> getFlussSplitsInfo(table));
        Assertions.assertTrue(error.getMessage().contains("were cleaned up before they could be leased"),
                error.getMessage());
        new Verifications() {
            {
                kvSnapshotLease.acquireSnapshots((Map<TableBucket, Long>) any);
                times = 1;
                kvSnapshotLease.dropLease();
                times = 1;
            }
        };
    }

    @Test
    public void testPrimaryKeyReadFailsAndDropsTheLeaseWhenLeasingFails() {
        mockLakeSourceUnused();
        mockPrimaryKeySnapshots();
        new Expectations() {
            {
                admin.createKvSnapshotLease(anyString, anyLong);
                result = kvSnapshotLease;
                kvSnapshotLease.acquireSnapshots((Map<TableBucket, Long>) any);
                result = CompletableFuture.failedFuture(new TimeoutException("coordinator timed out"));
                kvSnapshotLease.dropLease();
                result = CompletableFuture.completedFuture(null);
            }
        };
        FlussTable table = flussTable(tableInfo(false, true, true, "iceberg"), "");

        StarRocksConnectorException error =
                Assertions.assertThrows(StarRocksConnectorException.class, () -> getFlussSplitsInfo(table));
        Assertions.assertInstanceOf(TimeoutException.class, error.getCause());
        Assertions.assertTrue(error.getMessage().contains("coordinator timed out"), error.getMessage());
        new Verifications() {
            {
                kvSnapshotLease.dropLease();
                times = 1;
            }
        };
    }

    private void mockPrimaryKeySnapshots() {
        mockLatestOffsets(Map.of("", Map.of(0, 12L, 1, 3L, 2, 7L)));
        new Expectations() {
            {
                admin.getLatestKvSnapshots(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(new KvSnapshots(TABLE_ID, null,
                        mapOf(0, 5L, 1, null, 2, 6L), mapOf(0, 10L, 1, null, 2, 7L)));
            }
        };
    }

    /** Fluss grants the lease, except for the given buckets; returns what Fluss was asked to lease. */
    private List<Map<TableBucket, Long>> mockLease(Map<TableBucket, Long> unavailable) {
        List<Map<TableBucket, Long>> acquired = new ArrayList<>();
        new Expectations() {
            {
                admin.createKvSnapshotLease(anyString, anyLong);
                result = kvSnapshotLease;
                kvSnapshotLease.acquireSnapshots(withCapture(acquired));
                result = CompletableFuture.completedFuture(new AcquireKvSnapshotLeaseResult(unavailable));
                kvSnapshotLease.dropLease();
                result = CompletableFuture.completedFuture(null);
                minTimes = 0;
            }
        };
        return acquired;
    }

    private FlussSplitsInfo getFlussSplitsInfo(FlussTable table) {
        List<RemoteFileInfo> remoteFileInfos = metadata.getRemoteFiles(table, GetRemoteFilesParams.newBuilder().build());
        return ((FlussRemoteFileDesc) remoteFileInfos.get(0).getFiles().get(0)).getFlussSplitsInfo();
    }

    private void mockLakeSourceUnused() {
        new MockUp<LakeSourceUtils>() {
            @Mock
            public static LakeSource<LakeSplit> createLakeSource(TablePath tablePath, Map<String, String> options) {
                throw new AssertionError("A $rt read must not create a lake source");
            }
        };
    }

    /** Latest offsets keyed by partition name, with "" for an unpartitioned table; every log starts at 0. */
    private void mockLatestOffsets(Map<String, Map<Integer, Long>> offsetsByPartition) {
        new MockUp<BucketOffsetsRetrieverImpl>() {
            @Mock
            public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
                Map<Integer, Long> offsets = offsetsByPartition.get(partitionName == null ? "" : partitionName);
                Assertions.assertNotNull(offsets, "Unexpected offset request for partition " + partitionName);
                Assertions.assertEquals(offsets.keySet(), new HashSet<>(buckets));
                return offsets;
            }

            @Mock
            public Map<Integer, Long> earliestOffsets(String partitionName, Collection<Integer> buckets) {
                Map<Integer, Long> offsets = new HashMap<>();
                buckets.forEach(bucket -> offsets.put(bucket, 0L));
                return offsets;
            }
        };
    }

    private static Map<Integer, Long> mapOf(Integer k0, Long v0, Integer k1, Long v1, Integer k2, Long v2) {
        Map<Integer, Long> map = new HashMap<>();
        map.put(k0, v0);
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static void assertSnapshotLogSplit(SourceSplitBase split, Long partitionId, int bucket, long snapshotId,
                                               long logStartingOffset, long logStoppingOffset) {
        Assertions.assertTrue(split.isHybridSnapshotLogSplit(), "Expected a HybridSnapshotLogSplit but got " + split);
        HybridSnapshotLogSplit snapshotLogSplit = split.asHybridSnapshotLogSplit();
        Assertions.assertEquals(new TableBucket(TABLE_ID, partitionId, bucket), snapshotLogSplit.getTableBucket());
        Assertions.assertEquals(snapshotId, snapshotLogSplit.getSnapshotId());
        Assertions.assertEquals(logStartingOffset, snapshotLogSplit.getLogStartingOffset());
        Assertions.assertEquals(logStoppingOffset, snapshotLogSplit.getLogStoppingOffset().orElse(-1L));
        Assertions.assertTrue(snapshotLogSplit.isBatch());
    }

    private static void assertLogSplit(SourceSplitBase split, Long partitionId, int bucket,
                                       long startingOffset, long stoppingOffset) {
        Assertions.assertTrue(split.isLogSplit(), "Expected a LogSplit but got " + split);
        LogSplit logSplit = split.asLogSplit();
        Assertions.assertEquals(new TableBucket(TABLE_ID, partitionId, bucket), logSplit.getTableBucket());
        Assertions.assertEquals(startingOffset, logSplit.getStartingOffset());
        Assertions.assertEquals(stoppingOffset, logSplit.getStoppingOffset().orElse(-1L));
    }

    private void mockPaimonLakeWithoutSnapshot() {
        new MockUp<LakeSourceUtils>() {
            @Mock
            public static LakeSource<LakeSplit> createLakeSource(TablePath tablePath, Map<String, String> options) {
                return null;
            }
        };
        new Expectations() {
            {
                admin.getReadableLakeSnapshot(TablePath.of(DB, TABLE));
                result = CompletableFuture.failedFuture(new LakeTableSnapshotNotExistException("no snapshot"));
            }
        };
    }

    private List<SourceSplitBase> getFlussSplits(FlussTable table, GetRemoteFilesParams params) {
        List<RemoteFileInfo> remoteFileInfos = metadata.getRemoteFiles(table, params);
        Assertions.assertEquals(1, remoteFileInfos.size());
        List<RemoteFileDesc> remoteFileDescs = remoteFileInfos.get(0).getFiles();
        Assertions.assertEquals(1, remoteFileDescs.size());
        FlussRemoteFileDesc remoteFileDesc = (FlussRemoteFileDesc) remoteFileDescs.get(0);
        return remoteFileDesc.getFlussSplitsInfo().getFlussSplits();
    }

    private static FlussTable flussTable(TableInfo tableInfo, String tableNameSuffix) {
        FlussTable table = new FlussTable(CATALOG, DB, TABLE, schema(), tableInfo, new Configuration());
        table.setTableNameSuffix(tableNameSuffix);
        return table;
    }

    private static List<Column> schema() {
        return Arrays.asList(
                new Column("id", IntegerType.INT, true),
                new Column("name", TypeFactory.createDefaultCatalogString(), true),
                new Column("dt", TypeFactory.createDefaultCatalogString(), true),
                new Column("region", TypeFactory.createDefaultCatalogString(), true));
    }

    private static TableInfo tableInfo(boolean partitioned) {
        return tableInfo(partitioned, false);
    }

    private static TableInfo tableInfo(boolean partitioned, boolean primaryKey) {
        return tableInfo(partitioned, primaryKey, true);
    }

    private static TableInfo tableInfo(boolean partitioned, boolean primaryKey, boolean dataLakeEnabled) {
        return tableInfo(partitioned, primaryKey, dataLakeEnabled, "paimon");
    }

    private static TableInfo tableInfo(boolean partitioned, boolean primaryKey, boolean dataLakeEnabled,
                                       String dataLakeFormat) {
        Schema.Builder schemaBuilder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("dt", DataTypes.STRING())
                .column("region", DataTypes.STRING());
        if (primaryKey) {
            // A partitioned primary-key table must include its partition keys in the primary key.
            if (partitioned) {
                schemaBuilder.primaryKey("id", "dt", "region");
            } else {
                schemaBuilder.primaryKey("id");
            }
        }
        Schema schema = schemaBuilder.build();
        TableDescriptor.Builder descriptorBuilder = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3, "id")
                .comment("test fluss table")
                .property("table.datalake.enabled", Boolean.toString(dataLakeEnabled))
                .property("table.datalake.format", dataLakeFormat)
                .property("table.datalake.paimon.warehouse", "oss://warehouse");
        if (partitioned) {
            descriptorBuilder.partitionedBy("dt", "region");
        }
        return TableInfo.of(TablePath.of(DB, TABLE), TABLE_ID, 7, descriptorBuilder.build(),
                null, 1000L, 2000L);
    }

    private static PartitionInfo partitionInfo(long partitionId, String dt, String region) {
        return new PartitionInfo(partitionId,
                new ResolvedPartitionSpec(Lists.newArrayList("dt", "region"), Lists.newArrayList(dt, region)),
                null, 3);
    }
}
