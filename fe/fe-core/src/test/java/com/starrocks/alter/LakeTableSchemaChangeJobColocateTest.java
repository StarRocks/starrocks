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

package com.starrocks.alter;

import com.google.common.collect.Table;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ShardInfo;
import com.starrocks.alter.reshard.ColocateChecker;
import com.starrocks.alter.reshard.ReshardingMaterializedIndex;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A heavy schema change of a table in a range colocate group creates every shadow shard in the partition's index shard
 * group and in the PACK shard group of the ColocateRange its origin tablet lies in, pinned to its origin shard. Tables
 * outside range colocate groups keep the single index shard group.
 */
public class LakeTableSchemaChangeJobColocateTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        // Keeps the reshard daemon, and the colocate checker it runs, from acting on the tables of the tests.
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // The tests run the schema change jobs themselves; nothing may run them in the background.
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
    }

    @AfterAll
    public static void tearDown() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static Database db() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
    }

    private static StarOSAgent starOSAgent() {
        return GlobalStateMgr.getCurrentState().getStarOSAgent();
    }

    private static ColocateTableIndex colocateTableIndex() {
        return GlobalStateMgr.getCurrentState().getColocateTableIndex();
    }

    /** Creates a range table in the colocate group {@code colocateWith}; {@code orderBy} may be null. */
    private static void createColocateTable(String name, String columns, String keys, String orderBy,
                                            String colocateWith) throws Exception {
        starRocksAssert.withTable("create table " + name + " (" + columns + ")\n"
                + keys + "\n"
                + (orderBy == null ? "" : "order by(" + orderBy + ")\n")
                + "properties('replication_num' = '1', 'colocate_with' = '" + colocateWith + "');");
    }

    /** A tuple of INT values. */
    private static Tuple intTuple(Integer... values) {
        List<Variant> variants = new ArrayList<>(values.length);
        for (Integer value : values) {
            variants.add(Variant.of(IntegerType.INT, value.toString()));
        }
        return new Tuple(variants);
    }

    /**
     * Creates {@code name} in the colocate group {@code g_<name>:k1} after giving the group three ColocateRanges on k1,
     * (-inf, 100), [100, 200) and [200, +inf), each with its own PACK shard group, as earlier splits would have. A table
     * joining the group is created aligned with them, so every partition has one tablet per ColocateRange.
     */
    private static OlapTable createInThreeRangeGroup(String name, String columns, String keys) throws Exception {
        String group = "g_" + name;
        String seedName = name + "_seed";
        createColocateTable(seedName, "k1 int, v1 int", "duplicate key(k1)", null, group + ":k1");
        OlapTable seed = table(seedName);
        long grpId = colocateTableIndex().getGroup(seed.getId()).grpId;
        long firstShardGroupId = colocateTableIndex().getColocateRanges(grpId).get(0).getShardGroupId();
        long secondShardGroupId = starOSAgent().createShardGroup(db().getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        long thirdShardGroupId = starOSAgent().createShardGroup(db().getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        colocateTableIndex().getColocateRangeMgr().setColocateRanges(grpId, List.of(
                new ColocateRange(Range.lt(intTuple(100)), firstShardGroupId),
                new ColocateRange(Range.gelt(intTuple(100), intTuple(200)), secondShardGroupId),
                new ColocateRange(Range.ge(intTuple(200)), thirdShardGroupId)));
        createColocateTable(name, columns, keys, null, group + ":k1");
        // The seed still has a single tablet against the three ranges: no misaligned member may stay in the group.
        starRocksAssert.dropTable(seedName);
        return table(name);
    }

    private static List<ColocateRange> colocateRanges(OlapTable table) {
        return colocateTableIndex().getColocateRanges(colocateTableIndex().getGroup(table.getId()).grpId);
    }

    private static PhysicalPartition onlyPartition(OlapTable table) {
        assertEquals(1, table.getPhysicalPartitions().size());
        return table.getPhysicalPartitions().iterator().next();
    }

    private static List<Long> tabletIdsOf(MaterializedIndex index) {
        return index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList());
    }

    /** The shard groups StarOS has each of {@code shardIds} in. */
    private static Map<Long, List<Long>> shardGroupIdsOf(List<Long> shardIds) throws Exception {
        Map<Long, List<Long>> groupIds = new HashMap<>();
        for (ShardInfo shardInfo : starOSAgent().getShardInfo(shardIds, StarOSAgent.DEFAULT_WORKER_GROUP_ID)) {
            groupIds.put(shardInfo.getShardId(), List.copyOf(shardInfo.getGroupIdsList()));
        }
        assertEquals(new HashSet<>(shardIds), groupIds.keySet());
        return groupIds;
    }

    private static List<String> sortKeyNames(OlapTable table, long indexMetaId) {
        return MetaUtils.getRangeDistributionColumns(table, indexMetaId).stream()
                .map(Column::getName).collect(Collectors.toList());
    }

    /** The PACK shard group of the ColocateRange each tablet of {@code index} lies in, under the index's sort key. */
    private static Map<Long, Long> expectedPackShardGroupIds(OlapTable table, MaterializedIndex index) {
        List<ColocateRange> ranges = colocateRanges(table);
        ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(ranges,
                MetaUtils.getRangeDistributionColumns(table, index.getMetaId()), 1);
        Map<Long, Long> expected = new HashMap<>();
        for (Tablet tablet : index.getTablets()) {
            int rangeIndex = classifier.indexOf(tablet);
            assertTrue(rangeIndex >= 0, "tablet " + tablet.getId() + " " + tablet.getRange().getRange()
                    + " is in no single ColocateRange");
            expected.put(tablet.getId(), ranges.get(rangeIndex).getShardGroupId());
        }
        return expected;
    }

    /**
     * Asserts that StarOS has every tablet of {@code index} in the index shard group and the PACK shard group of the
     * ColocateRange the tablet lies in. Before the origin shards are deleted a tablet also carries the anonymous group
     * that pins it to its origin shard; {@code exactly} asserts there is nothing else, for after they are deleted.
     */
    private static void assertInIndexAndPackShardGroups(OlapTable table, MaterializedIndex index, boolean exactly)
            throws Exception {
        Map<Long, Long> expectedPack = expectedPackShardGroupIds(table, index);
        Map<Long, List<Long>> groupIds = shardGroupIdsOf(tabletIdsOf(index));
        for (Tablet tablet : index.getTablets()) {
            Set<Long> expected = Set.of(index.getShardGroupId(), expectedPack.get(tablet.getId()));
            List<Long> actual = groupIds.get(tablet.getId());
            String message = "tablet " + tablet.getId() + " " + tablet.getRange().getRange() + " is in " + actual
                    + ", expected the index shard group and PACK shard group " + expected;
            if (exactly) {
                assertEquals(expected, new HashSet<>(actual), message);
                assertEquals(2, actual.size(), message);
            } else {
                assertTrue(actual.containsAll(expected), message);
            }
        }
    }

    /** Runs ALTER {@code sql} and returns the heavy schema change job it registers; nothing runs it in the background. */
    private static LakeTableSchemaChangeJob submitHeavySchemaChange(OlapTable table, String sql) throws Exception {
        starRocksAssert.alterTable(sql);
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        List<AlterJobV2> jobs = handler.getUnfinishedAlterJobV2ByTableId(table.getId());
        handler.clearJobs();
        assertEquals(1, jobs.size());
        return assertInstanceOf(LakeTableSchemaChangeJob.class, jobs.get(0), "expected a shadow index schema change");
    }

    /** Runs a heavy schema change job to FINISHED: its tablet tasks report success and the flip's publish is skipped. */
    private static void runToFinished(LakeTableSchemaChangeJob job) throws Exception {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(task -> task.setFinished(true));
            }
        };
        new MockUp<AlterJobV2>() {
            @Mock
            public boolean publishVersion() {
                return true;
            }
        };
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        // PENDING spans several rounds: one dispatches the tablet creation tasks, the later ones poll them.
        while (job.getJobState() == AlterJobV2.JobState.PENDING) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "job never left PENDING");
            job.runPendingJob();
            Thread.sleep(10);
        }
        job.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "job never reached FINISHED");
            job.runFinishedRewritingJob();
            Thread.sleep(10);
        }
    }

    /** One StarOS call that creates shards with a group list of their own, each pinned to an old shard. */
    private record PinnedShardCreation(Map<Long, Long> newToOldShardId, Map<Long, List<Long>> newShardIdToGroupIds,
                                       boolean unpinPlacement) {
    }

    /** Records every call of {@link StarOSAgent#createShardsForSplit} and lets it create the shards. */
    private static List<PinnedShardCreation> recordPinnedShardCreations() {
        List<PinnedShardCreation> calls = new ArrayList<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Invocation invocation, Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds, FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo, Map<String, String> properties,
                                             ComputeResource computeResource, boolean unpinPlacement) {
                calls.add(new PinnedShardCreation(new LinkedHashMap<>(newToOldShardId),
                        new LinkedHashMap<>(newShardIdToGroupIds), unpinPlacement));
                invocation.proceed();
            }
        };
        return calls;
    }

    // ---------------------------------------------------------------------------------------------
    // Tables in a range colocate group
    // ---------------------------------------------------------------------------------------------

    /**
     * A value column type change rebuilds the base index. Each new tablet is created in the partition's index shard
     * group and in the PACK shard group of its ColocateRange, so once the origin shards are deleted, and with them the
     * anonymous group pinning each new shard to its origin shard, it is still kept with the other tablets of its range.
     */
    @Test
    public void testHeavySchemaChangePlacesShadowTabletsIntoTheirRangePackGroups() throws Exception {
        OlapTable table = createInThreeRangeGroup("t_sc_pack", "k1 int, k2 int, v1 int", "duplicate key(k1, k2)");
        PhysicalPartition partition = onlyPartition(table);
        MaterializedIndex origin = partition.getLatestBaseIndex();
        assertEquals(3, origin.getTablets().size(), "created aligned with the three ColocateRanges");
        assertInIndexAndPackShardGroups(table, origin, true);
        List<Long> originTabletIds = tabletIdsOf(origin);
        Set<Long> originPackShardGroupIds = new HashSet<>(expectedPackShardGroupIds(table, origin).values());

        runToFinished(submitHeavySchemaChange(table, "alter table t_sc_pack modify column v1 varchar(32)"));

        MaterializedIndex rebuilt = partition.getLatestBaseIndex();
        assertTrue(tabletIdsOf(rebuilt).stream().noneMatch(originTabletIds::contains), "the base index was rebuilt");
        assertEquals(origin.getShardGroupId(), rebuilt.getShardGroupId());
        assertEquals(originPackShardGroupIds, new HashSet<>(expectedPackShardGroupIds(table, rebuilt).values()));
        assertInIndexAndPackShardGroups(table, rebuilt, false);

        starOSAgent().deleteShards(new HashSet<>(originTabletIds));
        assertInIndexAndPackShardGroups(table, rebuilt, true);
    }

    /**
     * Within-range splits leave several tablets in one ColocateRange, and the tablet order of the index need not follow
     * the ranges. Each shadow shard is created pinned to the shard of the origin tablet it copies, in the PACK shard
     * group of that origin tablet's ColocateRange, and the job rewrites each origin tablet into that same shadow tablet.
     */
    @Test
    public void testHeavySchemaChangePairsEveryShadowShardWithItsOriginShard() throws Exception {
        OlapTable table = createInThreeRangeGroup("t_sc_pair", "k1 int, k2 int, v1 int", "duplicate key(k1, k2)");
        PhysicalPartition partition = onlyPartition(table);
        MaterializedIndex origin = partition.getLatestBaseIndex();
        List<ColocateRange> ranges = colocateRanges(table);
        // Tablet order A, A, B, A, C over the ColocateRanges A = (-inf, 100), B = [100, 200) and C = [200, +inf).
        List<Range<Tuple>> k1Ranges = List.of(Range.lt(intTuple(30)), Range.gelt(intTuple(60), intTuple(100)),
                Range.gelt(intTuple(100), intTuple(200)), Range.gelt(intTuple(30), intTuple(60)), Range.ge(intTuple(200)));
        List<Integer> rangeIndexes = List.of(0, 0, 1, 0, 2);
        for (long tabletId : tabletIdsOf(origin)) {
            origin.removeTablet(tabletId);
        }
        List<Column> sortKey = MetaUtils.getRangeDistributionColumns(table);
        TabletMeta tabletMeta = new TabletMeta(db().getId(), table.getId(), partition.getId(), origin.getId(),
                TStorageMedium.HDD, true);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(partition.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(origin.getId()));
        List<Long> expectedPack = new ArrayList<>();
        for (int i = 0; i < k1Ranges.size(); i++) {
            long packShardGroupId = ranges.get(rangeIndexes.get(i)).getShardGroupId();
            long shardId = starOSAgent().createShards(1, table.getPartitionFilePathInfo(partition.getId()),
                    table.getPartitionFileCacheInfo(partition.getId()),
                    List.of(origin.getShardGroupId(), packShardGroupId), null, properties,
                    WarehouseManager.DEFAULT_RESOURCE).get(0);
            origin.addTablet(new LakeTablet(shardId,
                    new TabletRange(ColocateRangeUtils.expandToFullSortKey(k1Ranges.get(i), sortKey, 1))), tabletMeta);
            expectedPack.add(packShardGroupId);
        }
        assertEquals(expectedPack, tabletIdsOf(origin).stream()
                .map(expectedPackShardGroupIds(table, origin)::get).collect(Collectors.toList()));
        List<PinnedShardCreation> calls = recordPinnedShardCreations();

        LakeTableSchemaChangeJob job = submitHeavySchemaChange(table, "alter table t_sc_pair modify column v1 varchar(32)");

        assertEquals(1, calls.size(), "one call per partition and index");
        PinnedShardCreation call = calls.get(0);
        assertFalse(call.unpinPlacement(), "each shadow shard stays pinned to its origin shard");
        Table<Long, Long, MaterializedIndex> shadowIndexes = Deencapsulation.getField(job, "physicalPartitionIndexMap");
        assertEquals(1, shadowIndexes.row(partition.getId()).size());
        long shadowIndexMetaId = shadowIndexes.row(partition.getId()).keySet().iterator().next();
        MaterializedIndex shadow = shadowIndexes.get(partition.getId(), shadowIndexMetaId);
        Table<Long, Long, Map<Long, Long>> tabletIdMaps = Deencapsulation.getField(job, "physicalPartitionIndexTabletMap");
        Map<Long, Long> shadowToOriginTablet = tabletIdMaps.get(partition.getId(), shadowIndexMetaId);
        assertEquals(tabletIdsOf(shadow), new ArrayList<>(call.newToOldShardId().keySet()));
        assertEquals(tabletIdsOf(origin), new ArrayList<>(call.newToOldShardId().values()));
        assertEquals(shadowToOriginTablet, call.newToOldShardId());
        assertEquals(shadow.getShardGroupId(), origin.getShardGroupId());
        for (int i = 0; i < k1Ranges.size(); i++) {
            Tablet shadowTablet = shadow.getTablets().get(i);
            assertEquals(origin.getTablets().get(i).getRange(), shadowTablet.getRange(), "position " + i);
            assertEquals(List.of(origin.getShardGroupId(), expectedPack.get(i)),
                    call.newShardIdToGroupIds().get(shadowTablet.getId()), "position " + i);
        }
        Map<Long, List<Long>> groupIds = shardGroupIdsOf(tabletIdsOf(shadow));
        for (int i = 0; i < k1Ranges.size(); i++) {
            assertTrue(groupIds.get(shadow.getTablets().get(i).getId())
                    .containsAll(List.of(origin.getShardGroupId(), expectedPack.get(i))), "position " + i);
        }
    }

    /** Runs ADD ROLLUP {@code rollup} on {@code table} through its range rollup job to FINISHED. */
    private static void addRangeRollup(OlapTable table, String rollup) throws Exception {
        MaterializedViewHandler rollupHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        try {
            starRocksAssert.alterTable("alter table " + table.getName() + " add rollup " + rollup);
            List<AlterJobV2> jobs = rollupHandler.getUnfinishedAlterJobV2ByTableId(table.getId());
            assertEquals(1, jobs.size());
            LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, jobs.get(0));
            // No sample: one rollup tablet per ColocateRange.
            job.setSampler(request -> SampleSet.EMPTY);
            job.runPendingJob();
            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            // The first round allocates the watershed; the loads before it are then reported finished.
            job.runWaitingTxnJob();
            new MockUp<LakeRangeRollupJob>() {
                @Mock
                public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                    return true;
                }
            };
            job.runWaitingTxnJob();
            assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
            markRewritesPublished(job);
            // The first round places the rollup tablets into the PACK shard groups and waits; the second finds them.
            job.runRunningJob();
            assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
            job.runRunningJob();
            assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
            job.setPublishExecutor(publishingJob -> true);
            job.runFinishedRewritingJob();
            assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        } finally {
            rollupHandler.clearJobs();
        }
    }

    /** Reports the rewrite of each partition of a RUNNING rollup job published, as the transaction of its INSERT would be. */
    private static void markRewritesPublished(LakeRangeRollupJob job) {
        long watershedTxnId = job.getTransactionId().orElseThrow();
        Map<Long, TransactionState> rewriteTxns = new HashMap<>();
        for (long physicalPartitionId : job.getPartitionIdsForTest()) {
            long rewriteTxnId = GlobalStateMgr.getCurrentState().getNextId();
            job.setRewriteTxnIdForTest(physicalPartitionId, rewriteTxnId);
            TransactionState rewriteTxn = new TransactionState(0L, new ArrayList<>(), rewriteTxnId, "rewrite", null,
                    TransactionState.LoadJobSourceType.SHADOW_REWRITE,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"), 0L, 60_000L);
            rewriteTxn.setTransactionStatus(TransactionStatus.VISIBLE);
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(0L);
            attachment.setShadowRewriteWatershedTxnId(watershedTxnId);
            attachment.setShadowRewriteAlterVersion(job.getWatershedVersion(physicalPartitionId));
            rewriteTxn.setTxnCommitAttachment(attachment);
            rewriteTxns.put(rewriteTxnId, rewriteTxn);
        }
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long transactionId) {
                TransactionState rewriteTxn = rewriteTxns.get(transactionId);
                return rewriteTxn != null ? rewriteTxn : invocation.proceed();
            }
        };
        job.setRewriteExecutor((context, insertStmt) -> {
        });
    }

    /**
     * A rollup sorted by (k1, k3) on a table sorted by (k1): the schema change rebuilds the rollup with the base index,
     * and each rebuilt rollup tablet is placed by the colocate prefix of its own range into its ColocateRange's PACK
     * shard group.
     */
    @Test
    public void testHeavySchemaChangeRebuildsRollupIntoPackGroups() throws Exception {
        OlapTable table = createInThreeRangeGroup("t_sc_rollup", "k1 int, k2 int, k3 int, v1 int", "duplicate key(k1)");
        addRangeRollup(table, "r (k1, k3, v1) order by (k1, k3)");
        PhysicalPartition partition = onlyPartition(table);
        long originRollupMetaId = table.getIndexMetaIdByName("r");
        MaterializedIndex originRollup = partition.getLatestIndex(originRollupMetaId);
        MaterializedIndex originBase = partition.getLatestBaseIndex();
        assertEquals(List.of("k1"), sortKeyNames(table, table.getBaseIndexMetaId()));
        assertEquals(List.of("k1", "k3"), sortKeyNames(table, originRollupMetaId));
        assertEquals(3, originRollup.getTablets().size());
        assertInIndexAndPackShardGroups(table, originRollup, true);
        Set<Long> originTabletIds = new HashSet<>(tabletIdsOf(originRollup));
        originTabletIds.addAll(tabletIdsOf(originBase));
        List<PinnedShardCreation> calls = recordPinnedShardCreations();

        runToFinished(submitHeavySchemaChange(table, "alter table t_sc_rollup modify column v1 varchar(32)"));

        assertEquals(2, calls.size(), "one call per partition and index: the base index and the rollup");
        long rollupMetaId = table.getIndexMetaIdByName("r");
        MaterializedIndex rollup = partition.getLatestIndex(rollupMetaId);
        assertTrue(tabletIdsOf(rollup).stream().noneMatch(originTabletIds::contains), "the rollup was rebuilt");
        assertEquals(List.of("k1", "k3"), sortKeyNames(table, rollupMetaId));
        MaterializedIndex base = partition.getLatestBaseIndex();
        assertInIndexAndPackShardGroups(table, rollup, false);
        assertInIndexAndPackShardGroups(table, base, false);

        starOSAgent().deleteShards(originTabletIds);
        assertInIndexAndPackShardGroups(table, rollup, true);
        assertInIndexAndPackShardGroups(table, base, true);
    }

    // ---------------------------------------------------------------------------------------------
    // Tables outside range colocate groups
    // ---------------------------------------------------------------------------------------------

    /** One call of the builder's single-group createShards: the index shard group and the origin shards to pin to. */
    private record SingleGroupShardCreation(long groupId, List<Long> matchShardIds) {
    }

    /** Records every call of the builder's single-group createShards and lets it create the shards. */
    private static List<SingleGroupShardCreation> recordSingleGroupShardCreations() {
        List<SingleGroupShardCreation> calls = new ArrayList<>();
        new MockUp<LakeTableAlterJobV2Builder>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int shardCount, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, long groupId, List<Long> matchShardIds,
                                           Map<String, String> properties, ComputeResource computeResource) {
                calls.add(new SingleGroupShardCreation(groupId, List.copyOf(matchShardIds)));
                return invocation.proceed();
            }
        };
        return calls;
    }

    /**
     * Runs a heavy schema change on {@code table} and asserts it creates the new shards as before: one call per partition
     * and index into the index shard group alone, pinned to the origin shards, and none through the per-shard group path.
     * The two lists are what {@link #recordSingleGroupShardCreations} and {@link #recordPinnedShardCreations} return;
     * they are cleared first.
     */
    private static void assertHeavySchemaChangeKeepsSingleShardGroup(OlapTable table,
                                                                     List<SingleGroupShardCreation> singleGroupCalls,
                                                                     List<PinnedShardCreation> perShardGroupCalls)
            throws Exception {
        PhysicalPartition partition = onlyPartition(table);
        MaterializedIndex origin = partition.getLatestBaseIndex();
        List<Long> originTabletIds = tabletIdsOf(origin);
        singleGroupCalls.clear();
        perShardGroupCalls.clear();

        runToFinished(submitHeavySchemaChange(table,
                "alter table " + table.getName() + " modify column v1 varchar(32)"));

        assertEquals(List.of(new SingleGroupShardCreation(origin.getShardGroupId(), originTabletIds)), singleGroupCalls);
        assertEquals(List.of(), perShardGroupCalls);
        MaterializedIndex rebuilt = partition.getLatestBaseIndex();
        List<Long> rebuiltTabletIds = tabletIdsOf(rebuilt);
        assertTrue(rebuiltTabletIds.stream().noneMatch(originTabletIds::contains), "the base index was rebuilt");
        Set<Long> packShardGroupIds = colocateTableIndex().getAllPackShardGroupIds();
        // Before the origin shards are deleted each new shard may also carry the anonymous group that pins it to its
        // origin shard, but no PACK shard group of a ColocateRange.
        for (List<Long> groupIds : shardGroupIdsOf(rebuiltTabletIds).values()) {
            assertTrue(groupIds.contains(rebuilt.getShardGroupId()), groupIds.toString());
            assertTrue(groupIds.size() <= 2, groupIds.toString());
            assertTrue(groupIds.stream().noneMatch(packShardGroupIds::contains), groupIds.toString());
        }
        starOSAgent().deleteShards(new HashSet<>(originTabletIds));
        for (List<Long> groupIds : shardGroupIdsOf(rebuiltTabletIds).values()) {
            assertEquals(List.of(rebuilt.getShardGroupId()), groupIds);
        }
    }

    /** A range table outside any colocate group and a hash colocate table keep the single index shard group. */
    @Test
    public void testHeavySchemaChangeOutsideRangeColocateGroupIsUnchanged() throws Exception {
        starRocksAssert.withTable("create table t_sc_plain_range (k1 int, k2 int, v1 int) duplicate key(k1, k2)\n"
                + "properties('replication_num' = '1');");
        OlapTable plainRange = table("t_sc_plain_range");
        assertTrue(plainRange.isRangeDistribution());
        assertFalse(colocateTableIndex().isColocateTable(plainRange.getId()));
        starRocksAssert.withTable("create table t_sc_hash_colocate (k1 int, k2 int, v1 int) duplicate key(k1, k2)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'g_sc_hash_colocate');");
        OlapTable hashColocate = table("t_sc_hash_colocate");
        assertTrue(colocateTableIndex().isColocateTable(hashColocate.getId()));
        assertNull(colocateTableIndex().getRangeColocateGroupId(hashColocate.getId()));
        // Mocked once for both tables: mocking the same method again within one test makes proceed() recurse.
        List<SingleGroupShardCreation> singleGroupCalls = recordSingleGroupShardCreations();
        List<PinnedShardCreation> perShardGroupCalls = recordPinnedShardCreations();

        assertHeavySchemaChangeKeepsSingleShardGroup(plainRange, singleGroupCalls, perShardGroupCalls);
        assertHeavySchemaChangeKeepsSingleShardGroup(hashColocate, singleGroupCalls, perShardGroupCalls);
    }

    // ---------------------------------------------------------------------------------------------
    // A split of a peer during the schema change
    // ---------------------------------------------------------------------------------------------

    private static List<TabletReshardJob> splitJobsOf(OlapTable table) {
        return GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().values().stream()
                .filter(job -> job.getJobType() == TabletReshardJob.JobType.SPLIT_TABLET)
                .filter(job -> job.getTableId() == table.getId())
                .collect(Collectors.toList());
    }

    /**
     * The shadow shards are placed by the ColocateRanges of the group when the job is created. A split of a peer that
     * adds the bound 150 afterwards marks the group unstable, and the colocate checker leaves the table alone while the
     * schema change runs. After the flip, the tablets that lie in one ColocateRange are in its PACK shard group, the
     * tablet that crosses the new bound is in the PACK shard group of the range it lay in, and the next checker cycle
     * submits an alignment split of that tablet.
     */
    @Test
    public void testPeerSplitDuringHeavySchemaChangeIsRealignedAfterFlip() throws Exception {
        OlapTable table = createInThreeRangeGroup("t_sc_peer_split", "k1 int, k2 int, v1 int", "duplicate key(k1, k2)");
        PhysicalPartition partition = onlyPartition(table);
        ColocateTableIndex.GroupId groupId = colocateTableIndex().getGroup(table.getId());
        List<ColocateRange> rangesAtCreation = colocateRanges(table);
        assertFalse(colocateTableIndex().isGroupUnstable(groupId));
        LakeTableSchemaChangeJob job =
                submitHeavySchemaChange(table, "alter table t_sc_peer_split modify column v1 varchar(32)");

        // What the publish of a peer's split at 150 does to the group.
        colocateTableIndex().applyRangeSplitResult(groupId.grpId, Set.of(intTuple(150)), 1,
                () -> starOSAgent().createShardGroup(db().getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK));

        assertEquals(4, colocateRanges(table).size());
        assertTrue(colocateTableIndex().isGroupUnstable(groupId));
        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();
        assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        assertTrue(colocateTableIndex().isGroupUnstable(groupId));
        assertEquals(List.of(), splitJobsOf(table));

        runToFinished(job);

        MaterializedIndex rebuilt = partition.getLatestBaseIndex();
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        List<ColocateRange> ranges = colocateRanges(table);
        ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(ranges,
                MetaUtils.getRangeDistributionColumns(table), 1);
        Map<Long, List<Long>> groupIds = shardGroupIdsOf(tabletIdsOf(rebuilt));
        List<Long> crossingTabletIds = new ArrayList<>();
        for (Tablet tablet : rebuilt.getTablets()) {
            int rangeIndex = classifier.indexOf(tablet);
            long expectedPack;
            if (rangeIndex < 0) {
                crossingTabletIds.add(tablet.getId());
                expectedPack = rangesAtCreation.get(1).getShardGroupId();
            } else {
                expectedPack = ranges.get(rangeIndex).getShardGroupId();
            }
            assertTrue(groupIds.get(tablet.getId()).containsAll(List.of(rebuilt.getShardGroupId(), expectedPack)),
                    "tablet " + tablet.getId() + " " + tablet.getRange().getRange() + " is in "
                            + groupIds.get(tablet.getId()) + ", not in " + expectedPack);
        }
        assertEquals(1, crossingTabletIds.size());
        assertTrue(colocateTableIndex().isGroupUnstable(groupId));

        checker.runOneCycle();

        List<TabletReshardJob> alignmentSplits = splitJobsOf(table);
        assertEquals(1, alignmentSplits.size());
        List<ReshardingMaterializedIndex> reshardingIndexes = new ArrayList<>(alignmentSplits.get(0)
                .getReshardingPhysicalPartitions().get(partition.getId()).getReshardingIndexes().values());
        assertEquals(1, reshardingIndexes.size());
        assertEquals(rebuilt.getId(), reshardingIndexes.get(0).getMaterializedIndexId());
        List<Long> splitTabletIds = reshardingIndexes.get(0).getReshardingTablets().stream()
                .filter(reshardingTablet -> reshardingTablet.getSplittingTablet() != null)
                .map(ReshardingTablet::getFirstOldTabletId).collect(Collectors.toList());
        assertEquals(crossingTabletIds, splitTabletIds);
    }
}
