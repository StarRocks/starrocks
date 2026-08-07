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

package com.starrocks.alter.reshard;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.alter.reshard.RangeDistributionMigrationService.RangeSpec;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class RangeDistributionMigrationServiceTest {
    private static StarRocksAssert starRocksAssert;
    private static AtomicInteger sequence;

    private Database database;
    private OlapTable table;
    private CapturedSubmission captured;
    private int oldMaxSplitCount;

    @BeforeAll
    static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        Config.enable_range_distribution = true;
        sequence = new AtomicInteger();
    }

    @BeforeEach
    void setUp() throws Exception {
        int suffix = sequence.incrementAndGet();
        String databaseName = "range_migration_service_" + suffix;
        String tableName = "tbl_" + suffix;
        starRocksAssert.withDatabase(databaseName).useDatabase(databaseName);
        starRocksAssert.withTable("create table " + tableName
                + " (partition_key int, k1 int, k2 bigint, v bigint) "
                + "duplicate key(partition_key, k1, k2) "
                + "partition by range(partition_key) (partition p0 values less than ('10'), "
                + "partition p1 values less than (MAXVALUE)) order by(k1, k2) "
                + "properties('replication_num'='1')");
        database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseName);
        table = (OlapTable) database.getTable(tableName);
        captured = new CapturedSubmission();
        oldMaxSplitCount = Config.tablet_reshard_max_split_count;
    }

    @AfterEach
    void tearDown() {
        Config.tablet_reshard_max_split_count = oldMaxSplitCount;
        if (table == null) {
            return;
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
        table.setType(TableType.CLOUD_NATIVE);
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        table.setColocateGroup(null);
    }

    @Test
    void topologyJsonIsDeterministicCompleteAndPreservesFullRanges() throws Exception {
        Tuple boundary = tuple(1, null);
        Tablet first = onlyTablet();
        first.setRange(new TabletRange(Range.of(null, boundary, false, true)));
        addTablet(latestIndex(), new TabletRange(Range.of(boundary, null, false, false)));
        addVisibleRollupToEveryPhysicalPartition();
        firstPhysical().updateVisibleVersion(7);
        secondPhysical().updateVisibleVersion(9);

        RangeDistributionMigrationService service = service();
        String firstJson = service.getTopology(database.getFullName(), table.getName());
        String secondJson = service.getTopology(database.getFullName(), table.getName());

        Assertions.assertEquals(firstJson, secondJson);
        JsonObject topology = JsonParser.parseString(firstJson).getAsJsonObject();
        Assertions.assertEquals(database.getFullName(), topology.get("databaseName").getAsString());
        Assertions.assertEquals(table.getName(), topology.get("tableName").getAsString());
        JsonArray partitions = topology.getAsJsonArray("partitions");
        Assertions.assertEquals(2, partitions.size());
        JsonObject physical = partitions.get(0).getAsJsonObject();
        Assertions.assertEquals(sortedPhysicalPartitions().get(0).getParentId(),
                physical.get("partitionId").getAsLong());
        Assertions.assertEquals(table.getPartition(physical.get("partitionId").getAsLong()).getName(),
                physical.get("partitionName").getAsString());
        Assertions.assertEquals(firstPhysical().getId(), physical.get("physicalPartitionId").getAsLong());
        Assertions.assertEquals(7, physical.get("visibleVersion").getAsLong());
        Assertions.assertEquals(2, physical.getAsJsonArray("indexes").size());
        JsonObject secondPhysicalJson = partitions.get(1).getAsJsonObject();
        Assertions.assertEquals(secondPhysical().getId(),
                secondPhysicalJson.get("physicalPartitionId").getAsLong());
        Assertions.assertEquals(9, secondPhysicalJson.get("visibleVersion").getAsLong());
        Assertions.assertEquals(2, secondPhysicalJson.getAsJsonArray("indexes").size());

        JsonObject baseIndex = findIndex(physical, table.getBaseIndexMetaId());
        Assertions.assertEquals(latestIndex().getId(), baseIndex.get("currentIndexId").getAsLong());
        Assertions.assertEquals(table.getName(), baseIndex.get("indexName").getAsString());
        JsonArray tablets = baseIndex.getAsJsonArray("tablets");
        Assertions.assertEquals(2, tablets.size());
        JsonObject firstRange = tablets.get(0).getAsJsonObject().getAsJsonObject("range");
        Assertions.assertTrue(firstRange.get("lowerBound").isJsonNull());
        Assertions.assertFalse(firstRange.get("lowerIncluded").getAsBoolean());
        Assertions.assertEquals("1", firstRange.getAsJsonArray("upperBound").get(0).getAsString());
        Assertions.assertTrue(firstRange.getAsJsonArray("upperBound").get(1).isJsonNull());
        Assertions.assertTrue(firstRange.get("upperIncluded").getAsBoolean());
        JsonObject secondRange = tablets.get(1).getAsJsonObject().getAsJsonObject("range");
        Assertions.assertFalse(secondRange.get("lowerIncluded").getAsBoolean());
        Assertions.assertTrue(secondRange.get("upperBound").isJsonNull());

        Assertions.assertEquals(firstJson, GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getRangeDistributionTopology(database.getFullName(), table.getName()));
    }

    @Test
    void completeMultiParentSubmissionUsesEachIndexSchemaAndExistingManager() throws Exception {
        MaterializedIndex base = latestIndex();
        MaterializedIndex rollup = addSingleColumnRollup(secondPhysical());
        Map<Long, List<RangeSpec>> request = new LinkedHashMap<>();
        request.put(base.getTablets().get(0).getId(), splitAt(List.of("5", "2")));
        request.put(rollup.getTablets().get(0).getId(), splitAt(List.of("7")));

        long jobId = service().submitSplit(database.getFullName(), table.getName(), request);

        Assertions.assertEquals(1234, jobId);
        Assertions.assertEquals(1, captured.factoryCalls);
        Assertions.assertEquals(1, captured.managerCalls);
        Assertions.assertEquals(new ArrayList<>(request.keySet()), new ArrayList<>(captured.ranges.keySet()));
        Tuple baseBoundary = captured.ranges.get(base.getTablets().get(0).getId())
                .get(0).getRange().getUpperBound();
        Assertions.assertEquals(table.getColumn("k1").getType(), baseBoundary.getValues().get(0).getType());
        Assertions.assertEquals(table.getColumn("k2").getType(), baseBoundary.getValues().get(1).getType());
        Tuple rollupBoundary = captured.ranges.get(rollup.getTablets().get(0).getId())
                .get(0).getRange().getUpperBound();
        Assertions.assertEquals(table.getColumn("k2").getType(), rollupBoundary.getValues().get(0).getType());
    }

    @Test
    void followerSubmissionFailsBeforePlanningOrMutation() {
        long parentId = onlyTablet().getId();
        RangeDistributionMigrationService followerService = new RangeDistributionMigrationService() {
            @Override
            protected boolean isLeaderAdmissionOpen() {
                return false;
            }

            @Override
            protected TabletReshardJob createSplitJob(Database ignoredDatabase, OlapTable ignoredTable,
                                                       Map<Long, List<TabletRange>> ranges) {
                captured.factoryCalls++;
                return Mockito.mock(TabletReshardJob.class);
            }

            @Override
            protected void addTabletReshardJob(TabletReshardJob job) {
                captured.managerCalls++;
            }
        };

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> followerService.submitSplit(database.getFullName(), table.getName(),
                        Map.of(parentId, splitAt(List.of("5", "2")))));

        Assertions.assertTrue(exception.getMessage().contains("active leader FE"));
        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    void unchangedMultiPartitionMultiIndexTopologyIsAdmittedAndPublishedAfterUnlock() throws Exception {
        addVisibleRollupToEveryPhysicalPartition();
        MaterializedIndex firstBase = firstPhysical().getLatestBaseIndex();
        MaterializedIndex secondRollup = secondPhysical()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).stream()
                .filter(index -> index.getMetaId() != table.getBaseIndexMetaId())
                .findFirst()
                .orElseThrow();
        Map<Long, List<RangeSpec>> request = new LinkedHashMap<>();
        request.put(firstBase.getTablets().get(0).getId(), splitAt(List.of("5", "2")));
        request.put(secondRollup.getTablets().get(0).getId(), splitAt(List.of("7")));

        TabletReshardJobMgr isolatedManager = new TabletReshardJobMgr();
        AtomicBoolean journaled = new AtomicBoolean();
        AtomicBoolean publishedBeforeJournal = new AtomicBoolean();
        AtomicReference<List<LockType>> journalLockTypes = new AtomicReference<>();
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(TabletReshardJob job) {
                publishedBeforeJournal.set(isolatedManager.getTabletReshardJob(job.getJobId()) == job);
                journalLockTypes.set(currentTableLockTypes());
                journaled.set(true);
            }
        };
        RangeDistributionMigrationService migrationService = new RangeDistributionMigrationService() {
            @Override
            protected void addTabletReshardJob(TabletReshardJob job) throws StarRocksException {
                isolatedManager.addTabletReshardJob(job);
            }
        };

        try {
            long jobId = migrationService.submitSplit(database.getFullName(), table.getName(), request);
            SplitTabletJob admittedJob = (SplitTabletJob) isolatedManager.getTabletReshardJob(jobId);
            Assertions.assertNotNull(admittedJob);
            Assertions.assertEquals(2, admittedJob.getReshardingPhysicalPartitions().size());
            List<ReshardingMaterializedIndex> admittedIndexes = admittedJob.getReshardingPhysicalPartitions()
                    .values().stream().flatMap(partition -> partition.getReshardingIndexes().values().stream())
                    .toList();
            Assertions.assertEquals(2, admittedIndexes.size());
            Assertions.assertTrue(admittedIndexes.stream()
                    .anyMatch(index -> index.getMaterializedIndexId() == firstBase.getId()));
            Assertions.assertTrue(admittedIndexes.stream()
                    .anyMatch(index -> index.getMaterializedIndexId() == secondRollup.getId()));
            Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
            Assertions.assertTrue(journaled.get());
            Assertions.assertTrue(publishedBeforeJournal.get());
            Assertions.assertEquals(List.of(), journalLockTypes.get());
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
            isolatedManager.getTabletReshardJobs().clear();
        }
    }

    @Test
    void sqlNullCellIsConvertedWithoutConflatingTransportString() throws Exception {
        long parentId = onlyTablet().getId();
        List<String> finite = new ArrayList<>();
        finite.add("1");
        finite.add(null);

        service().submitSplit(database.getFullName(), table.getName(), Map.of(parentId, splitAt(finite)));

        Tuple boundary = captured.ranges.get(parentId).get(0).getRange().getUpperBound();
        Assertions.assertEquals("1", boundary.getValues().get(0).getStringValue());
        Assertions.assertEquals(Variant.nullVariant(table.getColumn("k2").getType()), boundary.getValues().get(1));
    }

    @Test
    void submissionAcceptsOnlyHalfOpenChildRanges() throws Exception {
        Tablet parent = onlyTablet();
        long parentId = parent.getId();
        List<String> lower = List.of("0", "0");
        List<String> boundary = List.of("5", "2");
        List<String> upper = List.of("9", "9");

        parent.setRange(new TabletRange(Range.of(tuple(0, 0L), tuple(9, 9L), true, false)));
        service().submitSplit(database.getFullName(), table.getName(), Map.of(parentId, List.of(
                new RangeSpec(lower, true, boundary, false),
                new RangeSpec(boundary, true, upper, false))));
        List<TabletRange> leftClosed = captured.ranges.get(parentId);
        Assertions.assertTrue(leftClosed.get(0).getRange().isLowerBoundIncluded());
        Assertions.assertFalse(leftClosed.get(0).getRange().isUpperBoundIncluded());
        Assertions.assertTrue(leftClosed.get(1).getRange().isLowerBoundIncluded());
        Assertions.assertFalse(leftClosed.get(1).getRange().isUpperBoundIncluded());

        captured = new CapturedSubmission();
        parent.setRange(new TabletRange(Range.of(tuple(0, 0L), tuple(9, 9L), false, true)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(), Map.of(parentId, List.of(
                        new RangeSpec(lower, false, boundary, true),
                        new RangeSpec(boundary, false, upper, true)))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(), Map.of(parentId, List.of(
                        new RangeSpec(lower, false, boundary, false),
                        new RangeSpec(boundary, true, upper, false)))));
        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
    }

    @Test
    void visibleStringRollupPreservesMultiCellTopologyAndSubmission() throws Exception {
        String escaped = "quote\" slash\\ newline\n";
        List<String> mutable = new ArrayList<>();
        mutable.add("NULL");
        mutable.add(null);
        mutable.add(escaped);
        RangeSpec spec = new RangeSpec(mutable, true, null, false);
        mutable.set(0, "changed");
        Assertions.assertEquals("NULL", spec.lowerBound().get(0));
        Assertions.assertNull(spec.lowerBound().get(1));
        Assertions.assertEquals(escaped, spec.lowerBound().get(2));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> spec.lowerBound().set(0, "changed"));

        List<Column> stringColumns = List.of(
                new Column("string_key_1", VarcharType.VARCHAR),
                new Column("string_key_2", VarcharType.VARCHAR),
                new Column("string_key_3", VarcharType.VARCHAR));
        long metaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(metaId, "string_rollup_" + metaId, stringColumns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0, 1, 2));
        MaterializedIndex rollup = addRollupIndex(firstPhysical(), metaId);
        Tablet rollupTablet = rollup.getTablets().get(0);
        Tuple parentUpper = stringTuple(spec.lowerBound());
        rollupTablet.setRange(new TabletRange(Range.of(null, parentUpper, false, false)));

        JsonObject topology = JsonParser.parseString(
                new RangeDistributionMigrationService().getTopology(database.getFullName(), table.getName()))
                .getAsJsonObject();
        JsonObject physicalPartition = topology.getAsJsonArray("partitions").get(0).getAsJsonObject();
        JsonArray values = findIndex(physicalPartition, metaId)
                .getAsJsonArray("tablets").get(0).getAsJsonObject()
                .getAsJsonObject("range").getAsJsonArray("upperBound");
        Assertions.assertEquals("NULL", values.get(0).getAsString());
        Assertions.assertTrue(values.get(1).isJsonNull());
        Assertions.assertEquals(escaped, values.get(2).getAsString());

        List<String> splitBoundary = new ArrayList<>();
        splitBoundary.add("A");
        splitBoundary.add(null);
        splitBoundary.add("middle");
        service().submitSplit(database.getFullName(), table.getName(), Map.of(rollupTablet.getId(), List.of(
                new RangeSpec(null, false, splitBoundary, false),
                new RangeSpec(splitBoundary, true, spec.lowerBound(), false))));

        Tuple convertedBoundary = captured.ranges.get(rollupTablet.getId()).get(0).getRange().getUpperBound();
        Tuple convertedUpper = captured.ranges.get(rollupTablet.getId()).get(1).getRange().getUpperBound();
        for (int i = 0; i < stringColumns.size(); i++) {
            Assertions.assertEquals(stringColumns.get(i).getType(), convertedBoundary.getValues().get(i).getType());
            Assertions.assertEquals(stringColumns.get(i).getType(), convertedUpper.getValues().get(i).getType());
        }
        Assertions.assertEquals("A", convertedBoundary.getValues().get(0).getStringValue());
        Assertions.assertEquals(Variant.nullVariant(VarcharType.VARCHAR), convertedBoundary.getValues().get(1));
        Assertions.assertEquals("NULL", convertedUpper.getValues().get(0).getStringValue());
        Assertions.assertEquals(Variant.nullVariant(VarcharType.VARCHAR), convertedUpper.getValues().get(1));
        Assertions.assertEquals(escaped, convertedUpper.getValues().get(2).getStringValue());
    }

    @Test
    void topologyRejectsMinimumAndMaximumSentinelCells() {
        for (Variant sentinel : List.of(
                Variant.minVariant(table.getColumn("k1").getType()),
                Variant.maxVariant(table.getColumn("k1").getType()))) {
            Tuple endpoint = new Tuple(List.of(sentinel));
            onlyTablet().setRange(new TabletRange(Range.of(null, endpoint, false, false)));
            Assertions.assertThrows(IllegalStateException.class,
                    () -> new RangeDistributionMigrationService()
                            .getTopology(database.getFullName(), table.getName()));
        }
    }

    @Test
    void publicOperationsRejectNameReplacementAfterWaitingForOldTableLock() throws Exception {
        long parentId = onlyTablet().getId();
        RangeDistributionMigrationService migrationService = service();

        assertReplacementRejected(
                () -> migrationService.getTopology(database.getFullName(), table.getName()));
        assertReplacementRejected(
                () -> migrationService.submitSplit(database.getFullName(), table.getName(),
                        Map.of(parentId, splitAt(List.of("5", "2")))));

        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
    }

    @Test
    void splitPlanningUsesReadLockAndAdmissionRunsAfterServiceLockRelease() throws Exception {
        long parentId = onlyTablet().getId();
        RangeDistributionMigrationService migrationService = new RangeDistributionMigrationService() {
            @Override
            protected TabletReshardJob createSplitJob(Database ignoredDatabase, OlapTable ignoredTable,
                                                       Map<Long, List<TabletRange>> ranges) {
                Assertions.assertEquals(List.of(LockType.READ), currentTableLockTypes());
                captured.factoryCalls++;
                captured.ranges = ranges;
                TabletReshardJob job = Mockito.mock(TabletReshardJob.class);
                Mockito.when(job.getJobId()).thenReturn(1234L);
                return job;
            }

            @Override
            protected void addTabletReshardJob(TabletReshardJob job) {
                Assertions.assertTrue(currentTableLockTypes().isEmpty());
                captured.managerCalls++;
            }
        };

        Assertions.assertEquals(1234L, migrationService.submitSplit(database.getFullName(), table.getName(),
                Map.of(parentId, splitAt(List.of("5", "2")))));
        Assertions.assertEquals(1, captured.factoryCalls);
        Assertions.assertEquals(1, captured.managerCalls);
    }

    @Test
    void interveningTableStateChangeIsRejectedByExistingAdmissionPath() throws Exception {
        long parentId = onlyTablet().getId();
        TabletReshardJobMgr isolatedManager = new TabletReshardJobMgr();
        RangeDistributionMigrationService migrationService = new RangeDistributionMigrationService() {
            @Override
            protected void addTabletReshardJob(TabletReshardJob job) throws StarRocksException {
                Assertions.assertTrue(currentTableLockTypes().isEmpty());
                try (AutoCloseableLock ignored = new AutoCloseableLock(
                        database.getId(), table.getId(), LockType.WRITE)) {
                    table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
                }
                isolatedManager.addTabletReshardJob(job);
            }
        };

        try {
            Assertions.assertThrows(StarRocksException.class,
                    () -> migrationService.submitSplit(database.getFullName(), table.getName(),
                            Map.of(parentId, splitAt(List.of("5", "2")))));
            Assertions.assertTrue(isolatedManager.getTabletReshardJobs().isEmpty());
            Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
            isolatedManager.getTabletReshardJobs().clear();
        }
    }

    @Test
    void interveningTableReplacementIsRejectedWithoutAdmittingStaleJob() throws Exception {
        long parentId = onlyTablet().getId();
        String tableName = table.getName();
        OlapTable replacement = Mockito.mock(OlapTable.class);
        Mockito.when(replacement.getId()).thenReturn(GlobalStateMgr.getCurrentState().getNextId());
        Mockito.when(replacement.getName()).thenReturn(tableName);
        TabletReshardJobMgr isolatedManager = new TabletReshardJobMgr();
        RangeDistributionMigrationService migrationService = new RangeDistributionMigrationService() {
            @Override
            protected void addTabletReshardJob(TabletReshardJob job) throws StarRocksException {
                Assertions.assertTrue(currentTableLockTypes().isEmpty());
                Locker locker = new Locker();
                locker.lockDatabase(database.getId(), LockType.WRITE);
                try {
                    database.unRegisterTableUnlocked(table);
                    Assertions.assertTrue(database.registerTableUnlocked(replacement));
                } finally {
                    locker.unLockDatabase(database.getId(), LockType.WRITE);
                }
                isolatedManager.addTabletReshardJob(job);
            }
        };

        try {
            Assertions.assertThrows(StarRocksException.class,
                    () -> migrationService.submitSplit(database.getFullName(), tableName,
                            Map.of(parentId, splitAt(List.of("5", "2")))));
            Assertions.assertTrue(isolatedManager.getTabletReshardJobs().isEmpty());
            Assertions.assertSame(replacement, database.getTable(tableName));
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        } finally {
            Locker locker = new Locker();
            locker.lockDatabase(database.getId(), LockType.WRITE);
            try {
                if (database.getTable(tableName) == replacement) {
                    database.unRegisterTableUnlocked(replacement);
                }
                if (database.getTable(tableName) == null) {
                    Assertions.assertTrue(database.registerTableUnlocked(table));
                }
            } finally {
                locker.unLockDatabase(database.getId(), LockType.WRITE);
            }
            isolatedManager.getTabletReshardJobs().clear();
        }
    }

    @Test
    void malformedRangesAreRejectedBeforeFactoryOrManager() {
        long parentId = onlyTablet().getId();
        Config.tablet_reshard_max_split_count = 2;
        List<Map<Long, List<RangeSpec>>> invalid = List.of(
                Map.of(),
                Map.of(parentId, List.of()),
                Map.of(parentId, List.of(new RangeSpec(null, false, null, false))),
                Map.of(parentId, List.of(
                        spec(null, List.of("1", "1")), spec(List.of("1", "1"), List.of("2", "2")),
                        spec(List.of("2", "2"), null))),
                Map.of(parentId, List.of(
                        new RangeSpec(null, true, List.of("1", "1"), false),
                        spec(List.of("1", "1"), null))),
                Map.of(parentId, List.of(
                        spec(null, List.of("1", "1")),
                        new RangeSpec(List.of("1", "1"), true, null, true))),
                Map.of(parentId, List.of(spec(null, List.of("1")), spec(List.of("1"), null))),
                Map.of(parentId, List.of(spec(null, List.of("not-an-int", "1")),
                        spec(List.of("not-an-int", "1"), null))),
                Map.of(parentId, List.of(spec(null, List.of("2", "1")),
                        spec(List.of("1", "1"), null))),
                Map.of(parentId, List.of(spec(null, List.of("2", "1")),
                        spec(List.of("2", "1"), List.of("2", "1")))));

        for (Map<Long, List<RangeSpec>> request : invalid) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> service().submitSplit(database.getFullName(), table.getName(), request));
        }
        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
    }

    @Test
    void gapsOverlapsDuplicatesAndParentEscapeAreRejectedBeforeMutation() {
        Tablet parent = onlyTablet();
        parent.setRange(new TabletRange(Range.gelt(tuple(0, 0L), tuple(99, 9L))));
        List<List<RangeSpec>> invalid = List.of(
                List.of(spec(List.of("0", "0"), List.of("50", "1")),
                        spec(List.of("51", "1"), List.of("99", "9"))),
                List.of(new RangeSpec(List.of("0", "0"), true, List.of("50", "1"), false),
                        new RangeSpec(List.of("50", "1"), false, List.of("99", "9"), false)),
                List.of(new RangeSpec(List.of("0", "0"), true, List.of("50", "1"), true),
                        new RangeSpec(List.of("50", "1"), true, List.of("99", "9"), false)),
                List.of(spec(List.of("0", "0"), List.of("60", "1")),
                        spec(List.of("50", "1"), List.of("99", "9"))),
                List.of(spec(List.of("0", "0"), List.of("50", "1")),
                        spec(List.of("0", "0"), List.of("50", "1"))),
                List.of(spec(null, List.of("50", "1")), spec(List.of("50", "1"), List.of("99", "9"))),
                List.of(spec(List.of("0", "0"), List.of("50", "1")), spec(List.of("50", "1"), null)));

        for (List<RangeSpec> ranges : invalid) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> service().submitSplit(database.getFullName(), table.getName(),
                            Map.of(parent.getId(), ranges)));
        }
        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
    }

    @Test
    void busyStaleAndAlignedOneChildRetriesFailWithoutMutation() {
        long parentId = onlyTablet().getId();
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        Assertions.assertThrows(StarRocksException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(),
                        Map.of(parentId, splitAt(List.of("5", "2")))));
        table.setState(OlapTable.OlapTableState.NORMAL);

        StarRocksException missing = Assertions.assertThrows(StarRocksException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(), Map.of(
                        parentId + 1_000_000, splitAt(List.of("5", "2")),
                        parentId + 2_000_000, splitAt(List.of("7", "3")))));
        Assertions.assertTrue(missing.getMessage().contains(Long.toString(parentId + 1_000_000)));
        Assertions.assertTrue(missing.getMessage().contains(Long.toString(parentId + 2_000_000)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(),
                        Map.of(parentId, List.of(new RangeSpec(null, false, null, false)))));
        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
    }

    @Test
    void realFactoryAndIsolatedManagerRejectInProgressAndStaleRetries() throws Exception {
        Tablet parent = onlyTablet();
        long parentId = parent.getId();
        MaterializedIndex index = latestIndex();
        PhysicalPartition physicalPartition = firstPhysical();
        TabletReshardJobMgr isolatedManager = new TabletReshardJobMgr();
        RangeDistributionMigrationService service = new RangeDistributionMigrationService() {
            @Override
            protected void addTabletReshardJob(TabletReshardJob job) throws StarRocksException {
                isolatedManager.addTabletReshardJob(job);
            }
        };
        Map<Long, List<RangeSpec>> request = Map.of(parentId, splitAt(List.of("5", "2")));

        try {
            long jobId = service.submitSplit(database.getFullName(), table.getName(), request);
            Assertions.assertNotNull(isolatedManager.getTabletReshardJob(jobId));
            Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());

            Assertions.assertThrows(StarRocksException.class,
                    () -> service.submitSplit(database.getFullName(), table.getName(), request));
            Assertions.assertEquals(1, isolatedManager.getTabletReshardJobs().size());

            table.setState(OlapTable.OlapTableState.NORMAL);
            Assertions.assertSame(parent, index.removeTablet(parentId));
            Assertions.assertThrows(StarRocksException.class,
                    () -> service.submitSplit(database.getFullName(), table.getName(), request));
            Assertions.assertEquals(1, isolatedManager.getTabletReshardJobs().size());
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
            isolatedManager.getTabletReshardJobs().clear();
            if (index.getTablet(parentId) == null) {
                index.addTablet(parent, new TabletMeta(database.getId(), table.getId(), physicalPartition.getId(),
                        index.getId(), TStorageMedium.HDD, true), true);
            }
        }
    }

    @Test
    void localMetastorePublicSubmitUsesRealFactoryAndReentrantAdmissionLocks() throws Exception {
        long parentId = onlyTablet().getId();
        TabletReshardJobMgr manager = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        long[] jobId = {0};
        try {
            jobId[0] = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10),
                    () -> GlobalStateMgr.getCurrentState().getLocalMetastore().submitRangeDistributionSplit(
                            database.getFullName(), table.getName(),
                            Map.of(parentId, splitAt(List.of("5", "2")))));
            Assertions.assertNotNull(manager.getTabletReshardJob(jobId[0]));
            Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        } finally {
            if (jobId[0] != 0) {
                manager.getTabletReshardJobs().remove(jobId[0]);
            }
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    @Test
    void unsupportedTablesAndMissingNamesFailClosedBeforeMutation() {
        long parentId = onlyTablet().getId();
        Map<Long, List<RangeSpec>> request = Map.of(parentId, splitAt(List.of("5", "2")));

        table.setType(TableType.OLAP);
        Assertions.assertThrows(StarRocksException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(), request));
        table.setType(TableType.CLOUD_NATIVE);
        table.setDefaultDistributionInfo(new HashDistributionInfo());
        Assertions.assertThrows(StarRocksException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(), request));
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        table.setColocateGroup("unsupported");
        Assertions.assertThrows(StarRocksException.class,
                () -> service().submitSplit(database.getFullName(), table.getName(), request));
        table.setColocateGroup(null);
        Assertions.assertThrows(StarRocksException.class,
                () -> service().getTopology("missing_database", table.getName()));
        Assertions.assertThrows(StarRocksException.class,
                () -> service().getTopology(database.getFullName(), "missing_table"));
        Assertions.assertEquals(0, captured.factoryCalls);
        Assertions.assertEquals(0, captured.managerCalls);
    }

    private RangeDistributionMigrationService service() {
        return new RangeDistributionMigrationService() {
            @Override
            protected TabletReshardJob createSplitJob(Database ignoredDatabase, OlapTable ignoredTable,
                                                       Map<Long, List<TabletRange>> ranges) {
                captured.factoryCalls++;
                captured.ranges = ranges;
                TabletReshardJob job = Mockito.mock(TabletReshardJob.class);
                Mockito.when(job.getJobId()).thenReturn(1234L);
                return job;
            }

            @Override
            protected void addTabletReshardJob(TabletReshardJob job) {
                captured.managerCalls++;
            }
        };
    }

    private List<LockType> currentTableLockTypes() {
        return GlobalStateMgr.getCurrentState().getLockManager().dumpLockManager().stream()
                .filter(info -> info.getRid() == table.getId())
                .flatMap(info -> info.getOwners().stream())
                .map(holder -> holder.getLockType())
                .toList();
    }

    private void assertReplacementRejected(Callable<?> operation) throws Exception {
        String tableName = table.getName();
        OlapTable replacement = Mockito.mock(OlapTable.class);
        Mockito.when(replacement.getId()).thenReturn(GlobalStateMgr.getCurrentState().getNextId());
        Mockito.when(replacement.getName()).thenReturn(tableName);
        CompletableFuture<Throwable> result = null;

        try {
            try (AutoCloseableLock ignored = new AutoCloseableLock(
                    database.getId(), table.getId(), LockType.WRITE)) {
                result = CompletableFuture.supplyAsync(() -> {
                    try {
                        operation.call();
                        return null;
                    } catch (Throwable t) {
                        return t;
                    }
                });
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                        GlobalStateMgr.getCurrentState().getLockManager().dumpLockManager().stream()
                                .anyMatch(info -> info.getRid() == table.getId() && !info.getWaiters().isEmpty()));
                database.unRegisterTableUnlocked(table);
                Assertions.assertTrue(database.registerTableUnlocked(replacement));
            }

            Assertions.assertInstanceOf(StarRocksException.class, result.get(10, TimeUnit.SECONDS));
        } finally {
            if (result != null) {
                result.cancel(true);
            }
            if (database.getTable(tableName) == replacement) {
                database.unRegisterTableUnlocked(replacement);
            }
            if (database.getTable(tableName) == null) {
                Assertions.assertTrue(database.registerTableUnlocked(table));
            }
            Assertions.assertSame(table, database.getTable(tableName));
        }
    }

    private PhysicalPartition firstPhysical() {
        return sortedPhysicalPartitions().get(0);
    }

    private PhysicalPartition secondPhysical() {
        return sortedPhysicalPartitions().get(1);
    }

    private List<PhysicalPartition> sortedPhysicalPartitions() {
        List<PhysicalPartition> physicalPartitions = new ArrayList<>(table.getPhysicalPartitions());
        physicalPartitions.sort(Comparator.comparingLong(PhysicalPartition::getId));
        return physicalPartitions;
    }

    private MaterializedIndex latestIndex() {
        return firstPhysical().getLatestBaseIndex();
    }

    private Tablet onlyTablet() {
        Assertions.assertEquals(1, latestIndex().getTablets().size());
        Tablet tablet = latestIndex().getTablets().get(0);
        tablet.setRange(new TabletRange());
        return tablet;
    }

    private Tablet addTablet(MaterializedIndex index, TabletRange range) {
        long tabletId = GlobalStateMgr.getCurrentState().getNextId();
        Tablet tablet = new LakeTablet(tabletId, range);
        index.addTablet(tablet, new TabletMeta(database.getId(), table.getId(), firstPhysical().getId(),
                index.getId(), TStorageMedium.HDD, true), true);
        return tablet;
    }

    private void addVisibleRollupToEveryPhysicalPartition() {
        long metaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(metaId, "rollup_" + metaId, List.of(table.getColumn("k2")), 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0));
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            addRollupIndex(physicalPartition, metaId);
        }
    }

    private MaterializedIndex addSingleColumnRollup(PhysicalPartition physicalPartition) {
        long metaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(metaId, "rollup_" + metaId, List.of(table.getColumn("k2")), 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0));
        return addRollupIndex(physicalPartition, metaId);
    }

    private MaterializedIndex addRollupIndex(PhysicalPartition physicalPartition, long metaId) {
        long indexId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedIndex rollup = new MaterializedIndex(indexId, metaId, IndexState.NORMAL,
                physicalPartition.getShardGroupId());
        long tabletId = GlobalStateMgr.getCurrentState().getNextId();
        rollup.addTablet(new LakeTablet(tabletId, new TabletRange()),
                new TabletMeta(database.getId(), table.getId(), physicalPartition.getId(), indexId,
                        TStorageMedium.HDD, true), true);
        physicalPartition.createRollupIndex(rollup);
        return rollup;
    }

    private static JsonObject findIndex(JsonObject physicalPartition, long metaId) {
        for (var element : physicalPartition.getAsJsonArray("indexes")) {
            JsonObject index = element.getAsJsonObject();
            if (index.get("indexMetaId").getAsLong() == metaId) {
                return index;
            }
        }
        throw new AssertionError("index meta id not found: " + metaId);
    }

    private static List<RangeSpec> splitAt(List<String> boundary) {
        return List.of(spec(null, boundary), spec(boundary, null));
    }

    private static RangeSpec spec(List<String> lower, List<String> upper) {
        return new RangeSpec(lower, lower != null, upper, false);
    }

    private Tuple tuple(Integer first, Long second) {
        return new Tuple(List.of(
                Variant.of(table.getColumn("k1").getType(), first.toString()),
                second == null
                        ? Variant.nullVariant(table.getColumn("k2").getType())
                        : Variant.of(table.getColumn("k2").getType(), second.toString())));
    }

    private static Tuple stringTuple(List<String> values) {
        List<Variant> variants = new ArrayList<>(values.size());
        for (String value : values) {
            variants.add(value == null
                    ? Variant.nullVariant(VarcharType.VARCHAR)
                    : Variant.of(VarcharType.VARCHAR, value));
        }
        return new Tuple(List.copyOf(variants));
    }

    private static class CapturedSubmission {
        private int factoryCalls;
        private int managerCalls;
        private Map<Long, List<TabletRange>> ranges;
    }
}
