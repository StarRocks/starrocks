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

package com.starrocks.scheduler.mv.ivm;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An incremental refresh of an MV over an Iceberg table must record, in the MV version map, each partition's
 * metadata at the delta end it merged rather than at the table's current snapshot: a later PCT decision trusts
 * the record, so one that runs ahead of the data skips a partition whose changes never reached the MV.
 */
public class IvmIcebergVersionMapReadSnapshotTest extends MVIVMIcebergTestBase {
    private static final String PARTITIONED_QUERY = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
    private static final String UNPARTITIONED_QUERY = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`";
    private static final long DELTA_END = 2L;

    // snapshot id -> partition name -> the partition's metadata as that snapshot holds it
    private final Map<Long, Map<String, PartitionInfo>> snapshots = new HashMap<>();
    private final Set<Long> expiredSnapshots = new HashSet<>();

    @AfterEach
    public void dropMv() throws Exception {
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testIncrementalRunRecordsPartitionsAtItsDeltaEnd() throws Exception {
        MaterializedView mv = createIncrementalMv(PARTITIONED_QUERY, "`date`");
        IcebergTable t1 = icebergTable(MockIcebergMetadata.MOCKED_PARTITIONED_DB_NAME, "t1");
        servePartitionsAtSnapshots();
        commitSnapshot(DELTA_END, t1);
        loadPartition(t1, "date=2020-01-02");
        advanceTableVersionTo(DELTA_END);

        assertRunsIncrementally(mv);

        assertRecordedAt(DELTA_END, mv, t1, partitionNames(t1));
        Assertions.assertTrue(mv.getUpdatedPartitionNamesOfExternalTable(t1, false).contains("date=2020-01-02"),
                "the load past the delta end must still count as a change");
    }

    @Test
    public void testPartitionMissingAtTheDeltaEndIsNotRecorded() throws Exception {
        MaterializedView mv = createIncrementalMv(PARTITIONED_QUERY, "`date`");
        IcebergTable t1 = icebergTable(MockIcebergMetadata.MOCKED_PARTITIONED_DB_NAME, "t1");
        servePartitionsAtSnapshots();
        commitSnapshot(DELTA_END, t1);
        snapshots.get(DELTA_END).remove("date=2020-01-04");
        advanceTableVersionTo(DELTA_END);

        assertRunsIncrementally(mv);

        Assertions.assertFalse(recordedPartitions(mv, t1).containsKey("date=2020-01-04"),
                "a partition the delta end lacks has no version the refresh read");
        assertRecordedAt(DELTA_END, mv, t1, List.of("date=2020-01-01", "date=2020-01-02", "date=2020-01-03"));
    }

    @Test
    public void testUnreadableDeltaEndLeavesTheTableUnrecorded() throws Exception {
        MaterializedView mv = createIncrementalMv(PARTITIONED_QUERY, "`date`");
        IcebergTable t1 = icebergTable(MockIcebergMetadata.MOCKED_PARTITIONED_DB_NAME, "t1");
        servePartitionsAtSnapshots();
        expiredSnapshots.add(DELTA_END);
        advanceTableVersionTo(DELTA_END);

        assertRunsIncrementally(mv);

        Assertions.assertEquals(Map.of(), recordedPartitions(mv, t1),
                "the live metadata must not stand in for the delta end");
    }

    @Test
    public void testUnpartitionedTableRecordsItsDeltaEnd() throws Exception {
        MaterializedView mv = createIncrementalMv(UNPARTITIONED_QUERY, null);
        IcebergTable t0 = icebergTable(MockIcebergMetadata.MOCKED_UNPARTITIONED_DB_NAME, "t0");
        servePartitionsAtSnapshots();
        commitSnapshot(DELTA_END, t0);
        loadPartition(t0, t0.getCatalogTableName());
        advanceTableVersionTo(DELTA_END);

        assertRunsIncrementally(mv);

        assertRecordedAt(DELTA_END, mv, t0, partitionNames(t0));
    }

    @Test
    public void testPinnedBatchesKeepRecordingTheFrozenSnapshot() throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(PARTITIONED_QUERY, "auto", "`date`",
                Map.of("partition_refresh_number", "1", "partition_refresh_strategy", "strict"));
        IcebergTable t1 = icebergTable(MockIcebergMetadata.MOCKED_PARTITIONED_DB_NAME, "t1");
        servePartitionsAtSnapshots();
        commitSnapshot(DELTA_END, t1);
        advanceTableVersionTo(DELTA_END);

        TaskRun batch = withMVRefreshTaskRun(GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(mv.getDbId()).getFullName(), mv);
        Assertions.assertTrue(((MVHybridRefreshProcessor) refreshProcessorOf(batch)).getCurrentProcessor()
                instanceof MVPCTRefreshProcessor, "a missing baseline must send the job to pct");
        batch = nextOf(batch);
        Assertions.assertNotNull(batch, "partition_refresh_number=1 must split the job into batches");
        for (String name : partitionNames(t1)) {
            loadPartition(t1, name);
        }
        for (; batch != null; batch = nextOf(batch)) {
            initAndExecuteTaskRun(batch);
        }

        assertRecordedAt(DELTA_END, mv, t1, partitionNames(t1));
    }

    private MaterializedView createIncrementalMv(String query, String partitionBy) throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental", partitionBy, null);
        seedTvrBaselineAtVersionZero(mv);
        return mv;
    }

    private void assertRunsIncrementally(MaterializedView mv) throws Exception {
        MVTaskRunProcessor processor = getMVTaskRunProcessor(mv);
        Assertions.assertTrue(processor.getMVRefreshProcessor() instanceof MVIVMRefreshProcessor,
                "the refresh must run incrementally");
    }

    private void assertRecordedAt(long snapshotId, MaterializedView mv, IcebergTable table, List<String> names) {
        Map<String, MaterializedView.BasePartitionInfo> recorded = recordedPartitions(mv, table);
        Map<String, PartitionInfo> atSnapshot = snapshots.get(snapshotId);
        for (String name : names) {
            MaterializedView.BasePartitionInfo record = recorded.get(name);
            Assertions.assertNotNull(record, name + " is not recorded: " + recorded);
            Assertions.assertEquals(atSnapshot.get(name).getVersion(), record.getVersion(), name);
            Assertions.assertEquals(atSnapshot.get(name).getModifiedTime(), record.getLastRefreshTime(), name);
        }
    }

    private static Map<String, MaterializedView.BasePartitionInfo> recordedPartitions(MaterializedView mv,
                                                                                      IcebergTable table) {
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().entrySet().stream()
                .filter(entry -> entry.getKey().getDbName().equals(table.getCatalogDBName())
                        && entry.getKey().getTableName().equals(table.getCatalogTableName()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(Map.of());
    }

    /** Partition reads scoped to a committed snapshot see that snapshot; every other read sees the live table. */
    private void servePartitionsAtSnapshots() {
        new MockUp<MetadataMgr>() {
            @Mock
            public List<PartitionInfo> getPartitions(Invocation invocation, String catalogName, Table table,
                                                     List<String> partitionNames,
                                                     ConnectorMetadataRequestContext requestContext) {
                Map<String, PartitionInfo> atSnapshot = partitionsAt(requestContext);
                if (atSnapshot == null) {
                    return invocation.proceed();
                }
                if (table.isUnPartitioned()) {
                    return List.copyOf(atSnapshot.values());
                }
                // Like IcebergCatalog.getPartitionsByNames: a partition the snapshot lacks fails the whole read.
                ImmutableList.Builder<PartitionInfo> partitions = ImmutableList.builder();
                partitionNames.forEach(name -> partitions.add(atSnapshot.get(name)));
                return partitions.build();
            }

            @Mock
            public List<String> listPartitionNames(Invocation invocation, String catalogName, String dbName,
                                                   String tableName, ConnectorMetadataRequestContext requestContext) {
                Map<String, PartitionInfo> atSnapshot = partitionsAt(requestContext);
                if (atSnapshot == null) {
                    return invocation.proceed();
                }
                // Like IcebergCatalog.listPartitionNames, which lists nothing for an unpartitioned table.
                return MockIcebergMetadata.MOCKED_UNPARTITIONED_DB_NAME.equals(dbName)
                        ? List.of() : new ArrayList<>(atSnapshot.keySet());
            }
        };
    }

    private Map<String, PartitionInfo> partitionsAt(ConnectorMetadataRequestContext requestContext) {
        long snapshotId = requestContext.getSnapshotId();
        if (expiredSnapshots.contains(snapshotId)) {
            throw new StarRocksConnectorException("snapshot %s has expired", snapshotId);
        }
        return snapshots.get(snapshotId);
    }

    /** Records {@code table}'s live partitions as what {@code snapshotId} holds. */
    private void commitSnapshot(long snapshotId, IcebergTable table) {
        List<String> names = partitionNames(table);
        List<PartitionInfo> partitions = mockIcebergMetadata().getPartitions(table, names);
        Map<String, PartitionInfo> byName = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            byName.put(names.get(i), partitions.get(i));
        }
        snapshots.put(snapshotId, byName);
    }

    private static void loadPartition(IcebergTable table, String name) {
        mockIcebergMetadata().updatePartitions(table.getCatalogDBName(), table.getCatalogTableName(), List.of(name));
    }

    /** The mock keys an unpartitioned table's only partition by the table name. */
    private static List<String> partitionNames(IcebergTable table) {
        if (table.isUnPartitioned()) {
            return List.of(table.getCatalogTableName());
        }
        return mockIcebergMetadata().listPartitionNames(table.getCatalogDBName(), table.getCatalogTableName(),
                new ConnectorMetadataRequestContext());
    }

    private static IcebergTable icebergTable(String dbName, String tableName) {
        return (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME, dbName, tableName);
    }

    private static MockIcebergMetadata mockIcebergMetadata() {
        return (MockIcebergMetadata) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
    }

    private static MVRefreshProcessor refreshProcessorOf(TaskRun taskRun) {
        return getMVTaskRunProcessor(taskRun).getMVRefreshProcessor();
    }

    /** The batch {@code executed} spawned, or null when it was the job's last one. */
    private static TaskRun nextOf(TaskRun executed) {
        MVRefreshProcessor processor = refreshProcessorOf(executed);
        if (processor instanceof MVHybridRefreshProcessor hybrid) {
            return hybrid.getCurrentProcessor().getNextTaskRun();
        }
        return processor.getNextTaskRun();
    }
}
