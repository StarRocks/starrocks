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

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ShardInfo;
import com.starrocks.alter.reshard.ColocateChecker;
import com.starrocks.alter.reshard.ReshardingMaterializedIndex;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.presplit.BoundaryPlanner;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.InternalPartitionScanContext;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.alter.reshard.presplit.Sampler;
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
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.RangeColocateScanDispatch;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rollups of shared-data range tables in a range colocate group. The sort key of every index of such a table starts with
 * the group's colocate columns but can be shorter or longer than the base sort key, and each index is laid out against
 * the group's ColocateRanges in its own sort-key space: partition creation and the colocate scan dispatch expand the
 * ColocateRanges with the sort key of the index they work on.
 */
public class LakeRangeRollupColocateTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
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

    /** Creates a range table in the colocate group {@code colocateWith}; {@code orderBy} may be null. */
    private static void createColocateTable(String name, String columns, String keys, String orderBy,
                                            String colocateWith) throws Exception {
        starRocksAssert.withTable("create table " + name + " (" + columns + ")\n"
                + keys + "\n"
                + (orderBy == null ? "" : "order by(" + orderBy + ")\n")
                + "properties('replication_num' = '1', 'colocate_with' = '" + colocateWith + "');");
    }

    /** A tuple of INT values; a null element is a NULL. */
    private static Tuple intTuple(Integer... values) {
        List<Variant> variants = new ArrayList<>(values.length);
        for (Integer value : values) {
            variants.add(value == null ? Variant.nullVariant(IntegerType.INT) : Variant.of(IntegerType.INT, value.toString()));
        }
        return new Tuple(variants);
    }

    /**
     * Creates {@code name} in the colocate group {@code g_<name>:k1} after giving the group three ColocateRanges on k1,
     * (-inf, 100), [100, 200) and [200, +inf), each with its own PACK shard group, as earlier splits would have. A table
     * joining the group is created aligned with them, so every partition has one tablet per ColocateRange.
     */
    private static OlapTable createInThreeRangeGroup(String name, String columns, String keysAndPartitions,
                                                     String orderBy) throws Exception {
        String group = "g_" + name;
        String seedName = name + "_seed";
        createColocateTable(seedName, "k1 int, v1 int", "duplicate key(k1)", null, group + ":k1");
        OlapTable seed = table(seedName);
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateTableIndex.getGroup(seed.getId()).grpId;
        long firstShardGroupId = colocateTableIndex.getColocateRanges(grpId).get(0).getShardGroupId();
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        long secondShardGroupId = starOSAgent.createShardGroup(db().getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        long thirdShardGroupId = starOSAgent.createShardGroup(db().getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(grpId, List.of(
                new ColocateRange(Range.lt(intTuple(100)), firstShardGroupId),
                new ColocateRange(Range.gelt(intTuple(100), intTuple(200)), secondShardGroupId),
                new ColocateRange(Range.ge(intTuple(200)), thirdShardGroupId)));
        createColocateTable(name, columns, keysAndPartitions, orderBy, group + ":k1");
        // The seed still has a single tablet against the three ranges: no misaligned member may stay in the group.
        starRocksAssert.dropTable(seedName);
        return table(name);
    }

    /** DUP table (k1, k2, k3, v1) sorted by (k1, k2, k3) in a group with three ColocateRanges on k1. */
    private static OlapTable createDupTableInThreeRangeGroup(String name, String partitionClause) throws Exception {
        return createInThreeRangeGroup(name, "k1 int, k2 int, k3 int, v1 int",
                "duplicate key(k1, k2, k3)" + partitionClause, "k1, k2, k3");
    }

    private static List<ColocateRange> colocateRanges(OlapTable table) {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        return colocateTableIndex.getColocateRanges(colocateTableIndex.getGroup(table.getId()).grpId);
    }

    /** Keeps the reshard daemon, and the colocate checker it runs, from acting on the tables of a test. */
    private static void holdReshardDaemon() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
    }

    /**
     * Adds the meta of a DUP rollup {@code name} over the base columns {@code columnNames}, sorted by its first
     * {@code sortKeyLength} columns, and returns its meta id. No partition gets an index for it.
     */
    private static long injectRollupMeta(OlapTable table, String name, List<String> columnNames, int sortKeyLength) {
        List<Column> schema = new ArrayList<>();
        List<Integer> sortKeyIdxes = new ArrayList<>();
        List<Integer> sortKeyUniqueIds = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            Column column = new Column(table.getColumn(columnNames.get(i)));
            column.setIsKey(i < sortKeyLength);
            schema.add(column);
            if (i < sortKeyLength) {
                sortKeyIdxes.add(i);
                sortKeyUniqueIds.add(column.getUniqueId());
            }
        }
        long metaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(metaId, name, schema, 0, 0, (short) Math.min(sortKeyLength, 3), TStorageType.COLUMN,
                KeysType.DUP_KEYS, null, sortKeyIdxes, sortKeyUniqueIds);
        return metaId;
    }

    /**
     * Gives {@code partition} an index of the rollup {@code rollupMetaId} laid out as partition creation lays out an index
     * of a range colocate table: one tablet per ColocateRange, in range order, each with the range expanded in the
     * rollup's own sort key and its shard in the index SPREAD shard group and the range's PACK shard group.
     */
    private static MaterializedIndex injectAlignedRollupIndex(OlapTable table, PhysicalPartition partition,
                                                              long rollupMetaId) throws Exception {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        long spreadShardGroupId = starOSAgent.createShardGroup(db().getId(), table.getId(), partition.getId(),
                rollupMetaId);
        MaterializedIndex rollup = new MaterializedIndex(rollupMetaId, rollupMetaId,
                MaterializedIndex.IndexState.NORMAL, spreadShardGroupId);
        TabletMeta tabletMeta = new TabletMeta(db().getId(), table.getId(), partition.getId(), rollup.getId(),
                TStorageMedium.HDD, true);
        List<Column> rollupSortKey = MetaUtils.getRangeDistributionColumns(table, rollupMetaId);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(partition.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(rollup.getId()));
        for (ColocateRange colocateRange : colocateRanges(table)) {
            long shardId = starOSAgent.createShards(1, table.getPartitionFilePathInfo(partition.getId()),
                    table.getPartitionFileCacheInfo(partition.getId()),
                    List.of(spreadShardGroupId, colocateRange.getShardGroupId()), null, properties,
                    WarehouseManager.DEFAULT_RESOURCE).get(0);
            rollup.addTablet(new LakeTablet(shardId, new TabletRange(ColocateRangeUtils.expandToFullSortKey(
                    colocateRange.getRange(), rollupSortKey, 1))), tabletMeta);
        }
        partition.createRollupIndex(rollup);
        return rollup;
    }

    private static void assertAligned(OlapTable table, MaterializedIndex index, List<Column> sortKeyColumns) {
        assertTrue(RangeColocateScanDispatch.isTabletRangesAligned(index, sortKeyColumns, colocateRanges(table), 1),
                "not aligned with the colocate ranges: " + index.getTablets().stream()
                        .map(tablet -> String.valueOf(tablet.getRange().getRange())).collect(Collectors.toList()));
    }

    /** The shard groups StarOS has each tablet of {@code index} in. */
    private static Map<Long, List<Long>> shardGroupIdsOf(MaterializedIndex index) throws Exception {
        List<Long> tabletIds = index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList());
        Map<Long, List<Long>> groupIds = new HashMap<>();
        for (ShardInfo shardInfo : GlobalStateMgr.getCurrentState().getStarOSAgent()
                .getShardInfo(tabletIds, StarOSAgent.DEFAULT_WORKER_GROUP_ID)) {
            groupIds.put(shardInfo.getShardId(), shardInfo.getGroupIdsList());
        }
        assertEquals(new HashSet<>(tabletIds), groupIds.keySet());
        return groupIds;
    }

    // ---------------------------------------------------------------------------------------------
    // Partition creation and colocate scan bucketing of rollup indexes
    // ---------------------------------------------------------------------------------------------

    /**
     * ADD PARTITION creates the tablets of a rollup with the ColocateRanges expanded in the rollup's own sort key, shorter
     * or longer than the base sort key, and places each shard in the index SPREAD group and its range's PACK group.
     */
    @Test
    public void testAddPartitionCreatesRollupTabletsWithTheRollupSortKey() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_rollup_add_partition",
                "\npartition by range(k2) (partition p1 values less than ('100'))");
        long shorterMetaId = injectRollupMeta(table, "r_shorter", List.of("k1", "v1"), 1);
        long longerMetaId = injectRollupMeta(table, "r_longer", List.of("k1", "k2", "k3", "v1"), 4);

        starRocksAssert.alterTable("alter table t_rollup_add_partition add partition p2 values less than ('200')");

        PhysicalPartition partition = table.getPartition("p2").getDefaultPhysicalPartition();
        List<ColocateRange> ranges = colocateRanges(table);
        for (long rollupMetaId : List.of(shorterMetaId, longerMetaId)) {
            MaterializedIndex rollup = partition.getLatestIndex(rollupMetaId);
            assertNotNull(rollup);
            List<Column> rollupSortKey = MetaUtils.getRangeDistributionColumns(table, rollupMetaId);
            List<Tablet> tablets = rollup.getTablets();
            assertEquals(ranges.size(), tablets.size());
            for (int i = 0; i < ranges.size(); i++) {
                assertEquals(ColocateRangeUtils.expandToFullSortKey(ranges.get(i).getRange(), rollupSortKey, 1),
                        tablets.get(i).getRange().getRange(), "tablet " + i + " of rollup " + rollupMetaId);
            }
            assertAligned(table, rollup, rollupSortKey);
            Map<Long, List<Long>> groupIds = shardGroupIdsOf(rollup);
            for (int i = 0; i < ranges.size(); i++) {
                assertEquals(Set.of(rollup.getShardGroupId(), ranges.get(i).getShardGroupId()),
                        new HashSet<>(groupIds.get(tablets.get(i).getId())));
            }
        }
    }

    /**
     * The colocate scan dispatch expands the ColocateRanges with the sort key of the index it is given: a rollup laid out
     * in its own sort-key space, shorter or longer than the base sort key, gets one bucket per ColocateRange and passes
     * the dispatch-time alignment guard.
     */
    @Test
    public void testComputeBucketSeqUsesTheSelectedIndexSortKey() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_rollup_bucket_seq", "");
        long shorterMetaId = injectRollupMeta(table, "r_shorter", List.of("k1", "v1"), 1);
        long longerMetaId = injectRollupMeta(table, "r_longer", List.of("k1", "k2", "k3", "v1"), 4);
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        assertNotNull(dispatch);

        for (long rollupMetaId : List.of(shorterMetaId, longerMetaId)) {
            MaterializedIndex rollup = injectAlignedRollupIndex(table, partition, rollupMetaId);
            assertAligned(table, rollup, MetaUtils.getRangeDistributionColumns(table, rollupMetaId));
            List<Long> tabletIds = rollup.getTabletIdsInOrder();
            assertEquals(3, tabletIds.size());

            Map<Long, Integer> bucketSeq = dispatch.computeBucketSeq(rollup);

            assertEquals(Map.of(tabletIds.get(0), 0, tabletIds.get(1), 1, tabletIds.get(2), 2), bucketSeq,
                    "rollup " + rollupMetaId);
            dispatch.requireAligned(List.of(partition), rollupMetaId, bucketSeq);
        }
    }

    /** The base index is still bucketed with the base sort key when the table has a rollup of another length. */
    @Test
    public void testComputeBucketSeqOfBaseIndexUnchanged() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_rollup_base_bucket_seq", "");
        long rollupMetaId = injectRollupMeta(table, "r_shorter", List.of("k1", "v1"), 1);
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        injectAlignedRollupIndex(table, partition, rollupMetaId);
        MaterializedIndex baseIndex = partition.getLatestBaseIndex();
        ColocateRangeUtils.Classifier baseClassifier = ColocateRangeUtils.Classifier.of(colocateRanges(table),
                MetaUtils.getRangeDistributionColumns(table), 1);
        Map<Long, Integer> expected = new HashMap<>();
        for (Tablet tablet : baseIndex.getTablets()) {
            expected.put(tablet.getId(), baseClassifier.indexOf(tablet));
        }
        assertEquals(Set.of(0, 1, 2), new HashSet<>(expected.values()));
        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        assertNotNull(dispatch);

        Map<Long, Integer> bucketSeq = dispatch.computeBucketSeq(baseIndex);

        assertEquals(expected, bucketSeq);
        dispatch.requireAligned(List.of(partition), table.getBaseIndexMetaId(), bucketSeq);
    }

    // ---------------------------------------------------------------------------------------------
    // Admission of rollups and synchronous materialized views: the colocate prefix rule
    // ---------------------------------------------------------------------------------------------

    // Deliberately a literal, not the production constant: pins the user-visible wording.
    private static final String SYNC_MV_SORT_KEY_HINT = "The sort key of a synchronous materialized view is its "
            + "leading non-aggregated columns in SELECT order; for a view without GROUP BY, aggregation or DISTINCT it "
            + "is the short-key prefix of the SELECT list (at most 3 columns and 36 bytes; it ends with the first "
            + "VARCHAR column, or before the first column whose type cannot be a key, such as FLOAT or DOUBLE), "
            + "and ORDER BY does not change it. Use ALTER TABLE ... ADD ROLLUP ... ORDER BY (...) to choose the "
            + "sort key of such a projection.";

    /** The message of the colocate prefix rule for {@code subject}, without the hint of a synchronous view. */
    private static String prefixRuleMessage(String subject, String colocateColumns, String sortKey) {
        return "The sort key of " + subject + " in a range colocate group must start with its colocate columns "
                + colocateColumns + " (same order and types). Got: " + sortKey;
    }

    private static MaterializedViewHandler rollupHandler() {
        return GlobalStateMgr.getCurrentState().getRollupHandler();
    }

    /** Keeps the rollup handler from running the jobs of a test; the test drops them with clearJobs before it ends. */
    private static void holdRollupHandler() {
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
    }

    /** DUP table (k1, k2, k3, v1) sorted by (k1, k2, k3) in its own range colocate group {@code g_<name>:k1}. */
    private static OlapTable createDupColocateTable(String name) throws Exception {
        createColocateTable(name, "k1 int, k2 int, k3 int, v1 int", "duplicate key(k1, k2, k3)", "k1, k2, k3",
                "g_" + name + ":k1");
        return table(name);
    }

    private static void addRollup(String tableName, String rollup) throws Exception {
        starRocksAssert.alterTable("alter table " + tableName + " add rollup " + rollup);
    }

    private static void createSyncMv(String tableName, String mvName, String query) throws Exception {
        CreateSyncMVStmt stmt = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                "create materialized view " + mvName + " as " + query, connectContext);
        rollupHandler().processCreateMaterializedView(stmt, db(), table(tableName));
    }

    /** The one unfinished rollup job of {@code tableName}. */
    private static AlterJobV2 soleRollupJob(String tableName) {
        List<AlterJobV2> jobs = rollupHandler().getUnfinishedAlterJobV2ByTableId(table(tableName).getId());
        assertEquals(1, jobs.size());
        return jobs.get(0);
    }

    /** The message of the DdlException {@code executable} throws, directly or as the cause of what it throws. */
    private static String ddlErrorOf(Executable executable) {
        Throwable thrown = assertThrows(Throwable.class, executable);
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (current instanceof DdlException) {
                return current.getMessage();
            }
        }
        throw new AssertionError("no DdlException in the cause chain of " + thrown, thrown);
    }

    /** Asserts that {@code executable} fails with {@code message} and leaves the table NORMAL with no rollup job. */
    private static void assertRejected(String tableName, Executable executable, String message) {
        assertEquals(message, ddlErrorOf(executable));
        assertTrue(rollupHandler().getUnfinishedAlterJobV2ByTableId(table(tableName).getId()).isEmpty());
        assertEquals(OlapTable.OlapTableState.NORMAL, table(tableName).getState());
    }

    /**
     * ADD ROLLUP on a member of a range colocate group builds a range rollup job when the rollup's sort key starts with
     * the group's colocate columns, whether it is shorter or longer than the base sort key.
     */
    @Test
    public void testAddRollupKeepingColocatePrefixCreatesRollupJob() throws Exception {
        holdRollupHandler();
        try {
            String[][] cases = {
                    {"t_rollup_keep_short", "r (k1, v1) order by (k1)"},
                    {"t_rollup_keep_long", "r (k1, k3, k2) order by (k1, k3)"}};
            for (String[] c : cases) {
                OlapTable table = createDupColocateTable(c[0]);
                addRollup(c[0], c[1]);
                LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob(c[0]));
                assertEquals("r", job.getRollupIndexName());
                assertEquals(table.getColumn("k1").getUniqueId(), job.getShadowSortKeyUniqueIds().get(0));
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /** A rollup whose sort key does not start with the colocate column is rejected before any job is registered. */
    @Test
    public void testAddRollupBreakingColocatePrefixRejected() throws Exception {
        createDupColocateTable("t_rollup_break");
        String[][] cases = {
                {"r (k1, k2, v1) order by (k2, k1)", "[k2, k1]"},
                {"r (k1, k2, v1) order by (k2)", "[k2]"},
                {"r (k2, v1) order by (k2)", "[k2]"}};
        for (String[] c : cases) {
            assertRejected("t_rollup_break", () -> addRollup("t_rollup_break", c[0]),
                    prefixRuleMessage("rollup r of a table", "[k1]", c[1]));
        }
        assertNull(table("t_rollup_break").getIndexMetaIdByName("r"));
    }

    /** With two colocate columns the rollup's sort key must start with both, in the group's order. */
    @Test
    public void testMultiColumnColocatePrefixOrderEnforced() throws Exception {
        holdRollupHandler();
        try {
            createColocateTable("t_rollup_two_cols", "k1 int, k2 int, v int", "duplicate key(k1, k2)", "k1, k2",
                    "g_t_rollup_two_cols:k1,k2");
            String[][] cases = {
                    {"r (k1, k2, v) order by (k2, k1)", "[k2, k1]"},
                    {"r (k1, k2, v) order by (k1)", "[k1]"}};
            for (String[] c : cases) {
                assertRejected("t_rollup_two_cols", () -> addRollup("t_rollup_two_cols", c[0]),
                        prefixRuleMessage("rollup r of a table", "[k1, k2]", c[1]));
            }

            addRollup("t_rollup_two_cols", "r (k1, k2, v) order by (k1, k2, v)");

            assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_rollup_two_cols"));
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A synchronous materialized view whose derived sort key starts with the colocate column builds a range rollup job,
     * also when it renames the column.
     */
    @Test
    public void testSyncMvKeepingColocatePrefixCreatesRollupJob() throws Exception {
        holdRollupHandler();
        try {
            String[][] cases = {
                    {"t_sync_keep_agg", "select k1, k2, sum(v1) from t_sync_keep_agg group by k1, k2"},
                    {"t_sync_keep_alias", "select k1 as kk, v1 from t_sync_keep_alias"}};
            for (String[] c : cases) {
                OlapTable table = createDupColocateTable(c[0]);
                createSyncMv(c[0], "mv_" + c[0], c[1]);
                LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob(c[0]));
                assertTrue(job.isSyncMaterializedView());
                assertEquals(table.getColumn("k1").getUniqueId(), job.getShadowSortKeyUniqueIds().get(0));
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A synchronous materialized view whose sort key does not start with the colocate column is rejected, and the
     * message says how the sort key of such a view is derived: SELECT order decides the key order, and an expression on
     * the colocate column is another column.
     */
    @Test
    public void testSyncMvBreakingColocatePrefixRejectedWithKeyDerivationHint() throws Exception {
        holdRollupHandler();
        try {
            createDupColocateTable("t_sync_break");
            String subject = "synchronous materialized view mv_sync_break of a table";
            assertRejected("t_sync_break", () -> createSyncMv("t_sync_break", "mv_sync_break",
                            "select k2, k1, sum(v1) from t_sync_break group by k1, k2"),
                    prefixRuleMessage(subject, "[k1]", "[k2, k1]") + " " + SYNC_MV_SORT_KEY_HINT);
            String[] queries = {
                    "select k1 + 1 as kk, sum(v1) from t_sync_break group by k1 + 1",
                    "select abs(k1) as ka, v1 from t_sync_break",
                    "select cast(k1 as int) as kc, v1 from t_sync_break"};
            for (String query : queries) {
                String message = ddlErrorOf(() -> createSyncMv("t_sync_break", "mv_sync_break", query));
                assertTrue(message.startsWith(prefixRuleMessage(subject, "[k1]", "[")), message);
                assertTrue(message.endsWith("] " + SYNC_MV_SORT_KEY_HINT), message);
                assertTrue(rollupHandler().getUnfinishedAlterJobV2ByTableId(table("t_sync_break").getId()).isEmpty());
                assertEquals(OlapTable.OlapTableState.NORMAL, table("t_sync_break").getState());
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A plain projection takes the short-key prefix of its SELECT list as its sort key, which ends at the first VARCHAR
     * column: with a VARCHAR colocate column in front of another one it can not keep the colocate prefix and is
     * rejected with the hint, while ADD ROLLUP ... ORDER BY chooses a sort key that does.
     */
    @Test
    public void testPlainProjectionSyncMvOnVarcharLeadingGroupRejected() throws Exception {
        holdRollupHandler();
        try {
            createColocateTable("t_sync_varchar", "c varchar(16), k int, v int, w int", "duplicate key(c, k)", "c, k",
                    "g_t_sync_varchar:c,k");
            assertRejected("t_sync_varchar",
                    () -> createSyncMv("t_sync_varchar", "mv_sync_varchar", "select c, k, v from t_sync_varchar"),
                    prefixRuleMessage("synchronous materialized view mv_sync_varchar of a table", "[c, k]", "[c]")
                            + " " + SYNC_MV_SORT_KEY_HINT);

            addRollup("t_sync_varchar", "r (c, k, v) order by (c, k)");

            assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_sync_varchar"));
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /** SELECT DISTINCT makes every selected column a key, so it keeps four colocate columns in its sort key. */
    @Test
    public void testDistinctSyncMvOverFourColocateColumnsAccepted() throws Exception {
        holdRollupHandler();
        try {
            createColocateTable("t_sync_distinct", "a int, b int, c int, d int, v int", "duplicate key(a, b, c, d)",
                    "a, b, c, d", "g_t_sync_distinct:a,b,c,d");

            createSyncMv("t_sync_distinct", "mv_sync_distinct", "select distinct a, b, c, d from t_sync_distinct");

            LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_sync_distinct"));
            assertEquals(List.of(0, 1, 2, 3), job.getShadowSortKeyIdxes());
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /** CREATE TABLE ... ROLLUP with a rollup that keeps the colocate prefix creates the table in the group. */
    @Test
    public void testCreateTableRollupKeepingColocatePrefixAccepted() throws Exception {
        holdReshardDaemon();
        starRocksAssert.withTable("create table t_ctr_keep (k1 int, k2 int, v1 int)\n"
                + "order by(k1, k2)\n"
                + "rollup (r1 (k1, v1))\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'g_ctr_keep:k1');");

        OlapTable table = table("t_ctr_keep");
        assertNotNull(table);
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        assertNotNull(groupId);
        assertEquals(List.of(table.getId()), colocateTableIndex.getAllTableIds(groupId));
        Long rollupMetaId = table.getIndexMetaIdByName("r1");
        assertNotNull(rollupMetaId);
        List<Column> rollupSortKey = MetaUtils.getRangeDistributionColumns(table, rollupMetaId);
        assertEquals(List.of("k1", "v1"), rollupSortKey.stream().map(Column::getName).collect(Collectors.toList()));
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            assertAligned(table, partition.getLatestIndex(rollupMetaId), rollupSortKey);
        }
    }

    /**
     * CREATE TABLE ... ROLLUP with a rollup that breaks the colocate prefix is rejected before the table joins the group:
     * joining an existing group leaves its members and ColocateRanges as they were, and a new group is not created.
     */
    @Test
    public void testCreateTableRollupBreakingColocatePrefixRejectedLeavesNoMember() throws Exception {
        holdReshardDaemon();
        createColocateTable("t_ctr_member", "k1 int, k2 int, v1 int", "duplicate key(k1, k2)", "k1, k2", "g_ctr_join:k1");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(table("t_ctr_member").getId());
        List<Long> membersBefore = new ArrayList<>(colocateTableIndex.getAllTableIds(groupId));
        List<ColocateRange> rangesBefore = new ArrayList<>(colocateTableIndex.getColocateRanges(groupId.grpId));
        String ddl = "create table %s (k1 int, k2 int, v1 int)\n"
                + "order by(k1, k2)\n"
                + "rollup (r (k2, k1, v1))\n"
                + "properties('replication_num' = '1', 'colocate_with' = '%s:k1');";
        String expected = prefixRuleMessage("rollup r of a table", "[k1]", "[k2, k1, v1]")
                + " A rollup defined in CREATE TABLE is sorted by its key columns; an ORDER BY written in it is not used.";

        assertEquals(expected, ddlErrorOf(() -> starRocksAssert.withTable(String.format(ddl, "t_ctr_joiner", "g_ctr_join"))));

        assertNull(db().getTable("t_ctr_joiner"));
        assertEquals(membersBefore, colocateTableIndex.getAllTableIds(groupId));
        assertEquals(rangesBefore, colocateTableIndex.getColocateRanges(groupId.grpId));

        assertEquals(expected, ddlErrorOf(() -> starRocksAssert.withTable(String.format(ddl, "t_ctr_founder", "g_ctr_new"))));

        assertNull(db().getTable("t_ctr_founder"));
        assertNull(colocateTableIndex.getGroupSchema(ColocateTableIndex.getFullGroupName(db().getId(), "g_ctr_new")));
    }

    /**
     * A range table whose colocate_with is set but which is not a member of a colocate group keeps rejecting rollups and
     * synchronous materialized views: there are no ColocateRanges to lay them out against.
     */
    @Test
    public void testOrphanColocatePropertyKeepsRollupAndSyncMvRejected() throws Exception {
        starRocksAssert.withTable("create table t_orphan (k1 int, k2 int, k3 int, v1 int)\n"
                + "duplicate key(k1, k2, k3)\n"
                + "order by(k1, k2, k3)\n"
                + "properties('replication_num' = '1');");
        table("t_orphan").setColocateGroup("g_orphan:k1");

        assertRejected("t_orphan", () -> addRollup("t_orphan", "r (k1, v1) order by (k1)"),
                "ADD ROLLUP is not supported on this table with range distribution. A range distribution rollup is "
                        + "supported only on a shared-data, non-primary-key range table that is a member of its colocate "
                        + "group when it has colocate_with.");
        assertRejected("t_orphan",
                () -> createSyncMv("t_orphan", "mv_orphan", "select k1, sum(v1) from t_orphan group by k1"),
                "Synchronous materialized view with range distribution requires a shared-data, non-primary-key table "
                        + "that is a member of its colocate group when it has colocate_with. "
                        + "Use an asynchronous materialized view instead: declare it with an explicit "
                        + "REFRESH clause (REFRESH ASYNC or REFRESH MANUAL) or a DISTRIBUTED BY clause, "
                        + "e.g. CREATE MATERIALIZED VIEW ... DISTRIBUTED BY HASH(...) REFRESH ASYNC AS SELECT ...");
    }

    /**
     * The colocate prefix rule is checked again under the table lock before the shadow index is installed; a rollup
     * that no longer keeps the prefix there is cancelled and its shadow is removed. The check is made to see a sort key
     * other than the one the job plans with, as if the rollup's inputs had changed after submission.
     */
    @Test
    public void testPendingRollupCancelledWhenPrefixBreaksUnderLock() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupColocateTable("t_rollup_recheck");
            addRollup("t_rollup_recheck", "r (k1, k3, k2) order by (k1, k3)");
            LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_rollup_recheck"));
            long shadowMetaId = job.getShadowIndexMetaId();
            new MockUp<SchemaChangeHandler>() {
                @Mock
                List<Column> newRangeRoutingColumns(List<Column> newSchema, KeysType keysType,
                                                    List<Integer> sortKeyIdxes) {
                    List<Column> reversed = new ArrayList<>();
                    for (int idx : sortKeyIdxes) {
                        reversed.add(0, newSchema.get(idx));
                    }
                    return reversed;
                }
            };
            Set<Long> createdShadowShards = new HashSet<>();
            Set<Long> deletedShards = new HashSet<>();
            new MockUp<StarOSAgent>() {
                @Mock
                public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                               FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                               Map<String, String> properties, long metaGroupId,
                                               ComputeResource computeResource) {
                    List<Long> shardIds = invocation.proceed();
                    if (String.valueOf(shadowMetaId).equals(properties.get(LakeTablet.PROPERTY_KEY_INDEX_ID))) {
                        createdShadowShards.addAll(shardIds);
                    }
                    return shardIds;
                }

                @Mock
                public void deleteShards(Invocation invocation, Set<Long> shardIds) {
                    deletedShards.addAll(shardIds);
                    invocation.proceed();
                }
            };
            job.setSampler(request -> SampleSet.EMPTY);

            job.run();

            assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
            assertTrue(job.errMsg.contains(prefixRuleMessage("rollup r of a table", "[k1]", "[k3, k1]")), job.errMsg);
            assertNull(table.getIndexMetaByMetaId(shadowMetaId));
            assertNull(table.getIndexMetaIdByName("r"));
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                assertNull(partition.getLatestIndex(shadowMetaId));
            }
            assertFalse(createdShadowShards.isEmpty());
            assertTrue(deletedShards.containsAll(createdShadowShards), "shadow shards left: " + createdShadowShards);
            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * Hash colocate tables keep building the hash rollup job for ADD ROLLUP and synchronous materialized views, and a
     * range table outside any colocate group keeps building a range rollup job whatever its sort key starts with.
     */
    @Test
    public void testHashColocateRollupAndSyncMvUnchanged() throws Exception {
        holdRollupHandler();
        try {
            for (String name : List.of("t_hash_rollup", "t_hash_sync")) {
                starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, v1 int)\n"
                        + "duplicate key(k1, k2)\n"
                        + "distributed by hash(k1) buckets 2\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'g_hash_unchanged');");
            }
            addRollup("t_hash_rollup", "r (k2, k1)");
            assertEquals(LakeRollupJob.class, soleRollupJob("t_hash_rollup").getClass());
            createSyncMv("t_hash_sync", "mv_hash_sync", "select k2, sum(v1) from t_hash_sync group by k2");
            assertEquals(LakeRollupJob.class, soleRollupJob("t_hash_sync").getClass());

            for (String name : List.of("t_plain_rollup", "t_plain_sync")) {
                starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, v1 int)\n"
                        + "duplicate key(k1, k2)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1');");
            }
            addRollup("t_plain_rollup", "r (k2, v1) order by (k2)");
            assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_plain_rollup"));
            createSyncMv("t_plain_sync", "mv_plain_sync", "select k2, k1, sum(v1) from t_plain_sync group by k1, k2");
            assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_plain_sync"));
        } finally {
            rollupHandler().clearJobs();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Online rollup jobs: boundaries planned against the ColocateRanges, PACK placement, flip and replay
    // ---------------------------------------------------------------------------------------------

    private static final String TWO_PARTITIONS_BY_K2 = "\npartition by range(k2) "
            + "(partition p1 values less than ('100'), partition p2 values less than ('200'))";
    /** A rollup sorted by (k1, k3); a range rollup carries the partition column k2. */
    private static final String ROLLUP_K1_K3 = "r (k1, k3, k2) order by (k1, k3)";

    /** Runs ADD ROLLUP {@code rollup} on {@code tableName} and returns the range rollup job it registers. */
    private static LakeRangeRollupJob submitRollup(String tableName, String rollup) throws Exception {
        addRollup(tableName, rollup);
        return assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob(tableName));
    }

    /** A sampler returning canned tuples of the rollup's sort key. */
    private static Sampler stubSampler(List<Tuple> tuples) {
        return request -> tuples.isEmpty() ? SampleSet.EMPTY : new SampleSet(tuples, new Estimates(1024L, tuples.size()));
    }

    /**
     * A sample of the sort key (k1, k3), four tuples in each of the three ColocateRanges on k1: with six tablets
     * requested, each ColocateRange is cut in two, at (30, 1), (170, 1) and (300, 1).
     */
    private static Sampler sixTabletSampler() {
        return stubSampler(List.of(
                intTuple(10, 1), intTuple(20, 1), intTuple(30, 1), intTuple(40, 1),
                intTuple(120, 5), intTuple(150, 1), intTuple(170, 1), intTuple(180, 1),
                intTuple(200, 1), intTuple(250, 1), intTuple(300, 1), intTuple(350, 1)));
    }

    /** Makes the job ask for {@code tabletCount} tablets per partition, whatever the size of the partition. */
    private static void mockRequestedTabletCount(int tabletCount) {
        new MockUp<LakeOnlineRewriteJobBase>() {
            @Mock
            int selectRequestedTabletCount(LakeOnlineRewriteJobBase.PendingPartitionPlan plan, int activeComputeNodeCount,
                                           long targetSize) {
                return tabletCount;
            }
        };
    }

    /** The sort key of the rollup the job builds, as the index meta the job registered has it. */
    private static List<Column> rollupSortKey(OlapTable table, LakeRangeRollupJob job) {
        return MetaUtils.getRangeDistributionColumns(table, job.getShadowIndexMetaId());
    }

    private static List<String> columnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    /** Asserts that {@code index} has exactly one tablet inside each of the three ColocateRanges of the table's group. */
    private static void assertOneTabletPerColocateRange(OlapTable table, MaterializedIndex index,
                                                        List<Column> sortKeyColumns) {
        ColocateRangeUtils.Classifier classifier =
                ColocateRangeUtils.Classifier.of(colocateRanges(table), sortKeyColumns, 1);
        assertEquals(List.of(0, 1, 2), index.getTablets().stream()
                .map(tablet -> classifier.indexOf(tablet)).sorted().collect(Collectors.toList()));
    }

    /**
     * Asserts that StarOS has every tablet of {@code index} in exactly two shard groups: the index's own shard group
     * and the PACK shard group of the ColocateRange the tablet lies in.
     */
    private static void assertInPackGroupsOfColocateRanges(OlapTable table, MaterializedIndex index,
                                                           List<Column> sortKeyColumns) throws Exception {
        List<ColocateRange> ranges = colocateRanges(table);
        ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(ranges, sortKeyColumns, 1);
        Map<Long, List<Long>> groupIds = shardGroupIdsOf(index);
        for (Tablet tablet : index.getTablets()) {
            int rangeIndex = classifier.indexOf(tablet);
            assertTrue(rangeIndex >= 0, "tablet " + tablet.getId() + " " + tablet.getRange().getRange()
                    + " is in no single ColocateRange");
            assertEquals(Set.of(index.getShardGroupId(), ranges.get(rangeIndex).getShardGroupId()),
                    new HashSet<>(groupIds.get(tablet.getId())),
                    "tablet " + tablet.getId() + " " + tablet.getRange().getRange());
        }
    }

    /** Records what each PACK reconcile reports: true when it found every tablet placed. */
    private static List<Boolean> recordPackReconcileResults() {
        List<Boolean> results = new CopyOnWriteArrayList<>();
        new MockUp<ColocateChecker>() {
            @Mock
            public boolean reconcileTabletPackPlacement(Invocation invocation, Map<Long, Range<Tuple>> tabletIdToRange,
                                                        List<ColocateRange> expectedRanges, int colocateColumnCount,
                                                        String logContext) {
                boolean settled = invocation.proceed();
                results.add(settled);
                return settled;
            }
        };
        return results;
    }

    /** Runs the job from PENDING to RUNNING: the loads before the watershed are reported finished. */
    private static void runToRunning(LakeRangeRollupJob job) throws Exception {
        job.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        runWaitingTxnJobToRunning(job);
    }

    /** Runs a WAITING_TXN job to RUNNING: the loads before the watershed are reported finished. */
    private static void runWaitingTxnJobToRunning(LakeRangeRollupJob job) throws Exception {
        // The first round allocates the watershed and exposes the shadow index to loads.
        job.runWaitingTxnJob();
        new MockUp<LakeRangeRollupJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return true;
            }
        };
        job.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
    }

    /** Reports the rewrite of each partition of a RUNNING job published, as the transaction of its INSERT would be. */
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
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return rewriteTxns.get(transactionId);
            }
        };
        job.setRewriteExecutor((context, insertStmt) -> {
        });
    }

    /**
     * Runs a RUNNING job of a table in a range colocate group to FINISHED: the rewrite of each partition is reported
     * published, and the flip's publish to the compute nodes is skipped. The first round places the shadow tablets
     * into the PACK shard groups of their ColocateRanges and waits; the second finds them placed.
     */
    private static void runRunningJobToFinished(LakeRangeRollupJob job) throws Exception {
        markRewritesPublished(job);
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        job.setPublishExecutor(publishingJob -> true);
        job.runFinishedRewritingJob();
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
    }

    /** A copy of the job as a new leader reads it from the journal. */
    private static LakeRangeRollupJob takenOverByNewLeader(LakeRangeRollupJob job) {
        return (LakeRangeRollupJob) GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job.copyForPersist()), AlterJobV2.class);
    }

    /**
     * Asserts that the visible rollup {@code rollupMetaId} of every partition is NORMAL, aligned with the ColocateRanges
     * in its own sort key, in the PACK shard groups of its ColocateRanges, and bucketed by the colocate scan dispatch.
     */
    private static void assertVisibleRollupAlignedAndInPack(OlapTable table, long rollupMetaId) throws Exception {
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table, rollupMetaId);
        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        assertNotNull(dispatch);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            MaterializedIndex rollup = partition.getLatestIndex(rollupMetaId);
            assertNotNull(rollup, "partition " + partition.getId());
            assertEquals(MaterializedIndex.IndexState.NORMAL, rollup.getState());
            assertTrue(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).contains(rollup));
            assertAligned(table, rollup, sortKeyColumns);
            assertInPackGroupsOfColocateRanges(table, rollup, sortKeyColumns);
            assertNotNull(dispatch.computeBucketSeq(rollup), "not aligned: partition " + partition.getId());
        }
    }

    private static void assertGroupStable(OlapTable table) {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        assertFalse(colocateTableIndex.isGroupUnstable(colocateTableIndex.getGroup(table.getId())));
    }

    /**
     * The boundaries of the rollup are the ColocateRange bounds expanded into its sort key (k1, k3) plus row-quantile
     * cuts of the sample inside each ColocateRange. No duplicate boundary appears, and the canonical-shaped cut
     * (150, NULL) is dropped. Every partition is planned the same way and is aligned with the
     * ColocateRanges in the rollup's sort key.
     */
    @Test
    public void testRollupBoundariesUnionColocateBoundsAndSampledCuts() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_plan", TWO_PARTITIONS_BY_K2);
            LakeRangeRollupJob job = submitRollup("t_rollup_plan", ROLLUP_K1_K3);
            job.setSampler(stubSampler(List.of(
                    // (-inf, 100)
                    intTuple(10, 1), intTuple(20, 1), intTuple(30, 1), intTuple(40, 1),
                    // [100, 200), with the expanded bound (100, NULL) and the canonical-shaped (150, NULL)
                    intTuple(100, null), intTuple(120, 5), intTuple(150, null), intTuple(150, 1),
                    intTuple(170, 1), intTuple(180, 1), intTuple(190, 1), intTuple(199, 9),
                    // [200, +inf)
                    intTuple(200, 1), intTuple(250, 1), intTuple(300, 1), intTuple(350, 1))));
            mockRequestedTabletCount(6);

            job.runPendingJob();

            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            List<Column> sortKeyColumns = rollupSortKey(table, job);
            assertEquals(List.of("k1", "k3"), columnNames(sortKeyColumns));
            assertEquals(2, table.getPhysicalPartitions().size());
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                // The six requested tablets are shared 1:2:1 by sample size, so the ranges ask for 2, 3 and 2 tablets;
                // the middle range loses its canonical-shaped cut (150, NULL).
                assertEquals(List.of(intTuple(30, 1), intTuple(100, null), intTuple(180, 1), intTuple(200, null),
                        intTuple(300, 1)), job.getBoundaries(partition.getId()), "partition " + partition.getId());
                assertAligned(table, job.getShadowIndex(partition.getId()), sortKeyColumns);
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /** When the rollup's sort key is the colocate columns, every cut would have the shape of a ColocateRange bound. */
    @Test
    public void testRollupSortKeyOfColocateColumnsGivesOneTabletPerColocateRange() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_whole_key", "");
            LakeRangeRollupJob job = submitRollup("t_rollup_whole_key", "r (k1, v1) order by (k1)");
            job.setSampler(request -> {
                throw new AssertionError("no sample can split a ColocateRange of a sort key made of the colocate columns");
            });
            mockRequestedTabletCount(6);

            job.runPendingJob();

            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            List<Column> sortKeyColumns = rollupSortKey(table, job);
            assertEquals(List.of("k1"), columnNames(sortKeyColumns));
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            assertEquals(List.of(intTuple(100), intTuple(200)), job.getBoundaries(partition.getId()));
            assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), sortKeyColumns);
            assertAligned(table, job.getShadowIndex(partition.getId()), sortKeyColumns);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A partition whose sampling fails and a partition whose sample is empty each get one rollup tablet per
     * ColocateRange.
     */
    @Test
    public void testRollupSamplingFailureAndEmptyPartitionGiveOneTabletPerColocateRange() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_no_sample", TWO_PARTITIONS_BY_K2);
            LakeRangeRollupJob job = submitRollup("t_rollup_no_sample", ROLLUP_K1_K3);
            Set<String> sampledPartitions = new HashSet<>();
            job.setSampler(request -> {
                String partitionName = ((InternalPartitionScanContext) request.getScanContext()).partitionName();
                sampledPartitions.add(partitionName);
                if (partitionName.equals("p1")) {
                    throw new StarRocksException("sampling failed");
                }
                return SampleSet.EMPTY;
            });
            mockRequestedTabletCount(6);

            job.runPendingJob();

            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            assertEquals(Set.of("p1", "p2"), sampledPartitions);
            List<Column> sortKeyColumns = rollupSortKey(table, job);
            for (String partitionName : List.of("p1", "p2")) {
                PhysicalPartition partition = table.getPartition(partitionName).getDefaultPhysicalPartition();
                assertEquals(List.of(intTuple(100, null), intTuple(200, null)), job.getBoundaries(partition.getId()),
                        partitionName);
                assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), sortKeyColumns);
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /** With automatic resharding off, each ColocateRange keeps the number of base tablets it has. */
    @Test
    public void testRollupPreserveModeKeepsBaseTabletCountOfEachColocateRange() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        long savedTargetSize = Config.tablet_reshard_target_size;
        Config.tablet_reshard_target_size = 0;
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_preserve", "");
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            // Split the base tablet of [100, 200) in two, as a split of that tablet would have.
            MaterializedIndex baseIndex = partition.getLatestBaseIndex();
            Tablet middleTablet = baseIndex.getTablets().stream()
                    .filter(tablet -> !tablet.getRange().getRange().isMinimum()
                            && tablet.getRange().getRange().getLowerBound().compareTo(intTuple(100, null, null)) == 0)
                    .findFirst().orElseThrow();
            middleTablet.setRange(new TabletRange(Range.gelt(intTuple(100, null, null), intTuple(150, 1, 1))));
            baseIndex.addTablet(new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(),
                    new TabletRange(Range.gelt(intTuple(150, 1, 1), intTuple(200, null, null)))), null, false);
            LakeRangeRollupJob job = submitRollup("t_rollup_preserve", "r (k1, k3) order by (k1, k3)");
            job.setSampler(stubSampler(List.of(intTuple(10, 1), intTuple(20, 1),
                    intTuple(150, 1), intTuple(160, 1), intTuple(170, 1), intTuple(180, 1),
                    intTuple(210, 1), intTuple(220, 1))));

            job.runPendingJob();

            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            assertEquals(List.of(intTuple(100, null), intTuple(170, 1), intTuple(200, null)),
                    job.getBoundaries(partition.getId()));
            assertAligned(table, job.getShadowIndex(partition.getId()), rollupSortKey(table, job));
        } finally {
            Config.tablet_reshard_target_size = savedTargetSize;
            rollupHandler().clearJobs();
        }
    }

    /** A rollup shadow index that is not aligned with the ColocateRanges is never installed: the job is cancelled. */
    @Test
    public void testPendingRollupCancelledWhenShadowIsNotAligned() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_unaligned", "");
            LakeRangeRollupJob job = submitRollup("t_rollup_unaligned", "r (k1, k3) order by (k1, k3)");
            long shadowMetaId = job.getShadowIndexMetaId();
            job.setSampler(stubSampler(List.of(intTuple(150, 1), intTuple(160, 1))));
            mockRequestedTabletCount(6);
            // A planning defect that loses the ColocateRange bounds.
            new MockUp<RangeColocateRewriteLayout>() {
                @Mock
                List<Tuple> planBoundaries(List<List<Tuple>> samplesByRange, int[] tabletCounts) {
                    return List.of(intTuple(160, 1));
                }
            };

            job.run();

            assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
            assertTrue(job.errMsg.contains("is not aligned with the colocate ranges"), job.errMsg);
            assertNull(table.getIndexMetaByMetaId(shadowMetaId));
            assertNull(table.getIndexMetaIdByName("r"));
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                assertNull(partition.getLatestIndex(shadowMetaId));
            }
            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * The shards of the rollup tablets are created in the partition's shard group only. Once every partition is
     * rewritten, the job places each of them into the PACK shard group of its ColocateRange and stays RUNNING, saying
     * what it waits for, until a later round finds them all placed.
     */
    @Test
    public void testRollupShardsSpreadOnlyAtCreationAndPackBeforeFinishingRewrite() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_pack", "");
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            LakeRangeRollupJob job = submitRollup("t_rollup_pack", "r (k1, k3) order by (k1, k3)");
            job.setSampler(sixTabletSampler());
            mockRequestedTabletCount(6);

            job.runPendingJob();

            MaterializedIndex shadowIndex = job.getShadowIndex(partition.getId());
            assertEquals(6, shadowIndex.getTablets().size());
            long partitionShardGroupId = partition.getLatestBaseIndex().getShardGroupId();
            assertEquals(partitionShardGroupId, shadowIndex.getShardGroupId());
            assertEquals(Set.of(List.of(partitionShardGroupId)), new HashSet<>(shardGroupIdsOf(shadowIndex).values()));
            runWaitingTxnJobToRunning(job);
            assertEquals(Set.of(List.of(partitionShardGroupId)), new HashSet<>(shardGroupIdsOf(shadowIndex).values()));
            markRewritesPublished(job);
            List<Boolean> reconcileResults = recordPackReconcileResults();

            job.runRunningJob();

            assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
            assertEquals(List.of(false), reconcileResults);
            assertEquals("every partition is rewritten, waiting for PACK placement of 6 shadow tablets", job.errMsg);
            List<Column> sortKeyColumns = rollupSortKey(table, job);
            assertInPackGroupsOfColocateRanges(table, shadowIndex, sortKeyColumns);

            job.runRunningJob();

            assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
            assertEquals(List.of(false, true), reconcileResults);
            assertEquals("", job.errMsg);
            assertInPackGroupsOfColocateRanges(table, shadowIndex, sortKeyColumns);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * After the flip the rollup is a visible index of every partition, aligned with the ColocateRanges in its own sort
     * key, in the PACK shard groups of its ColocateRanges and bucketed by the colocate scan dispatch, and the group
     * stays stable.
     */
    @Test
    public void testRollupFlipIsAlignedInPackAndGroupStable() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_flip", TWO_PARTITIONS_BY_K2);
            LakeRangeRollupJob job = submitRollup("t_rollup_flip", ROLLUP_K1_K3);
            job.setSampler(sixTabletSampler());
            mockRequestedTabletCount(6);

            runToRunning(job);
            runRunningJobToFinished(job);

            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Long rollupMetaId = table.getIndexMetaIdByName("r");
            assertEquals(job.getShadowIndexMetaId(), rollupMetaId);
            assertEquals(List.of("k1", "k3"), columnNames(MetaUtils.getRangeDistributionColumns(table, rollupMetaId)));
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                assertEquals(6, partition.getLatestIndex(rollupMetaId).getTablets().size());
            }
            assertVisibleRollupAlignedAndInPack(table, rollupMetaId);
            assertGroupStable(table);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * Records every alter job entry written to the edit log from now on, serialized when it is written, which is what
     * a follower reads. ADD ROLLUP writes its first entry as a batch of jobs.
     */
    private static List<String> recordAlterJobJournal() {
        List<String> journal = new CopyOnWriteArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logAlterJob(Invocation invocation, AlterJobV2 alterJob, WALApplier walApplier) {
                String entry = GsonUtils.GSON.toJson(alterJob, AlterJobV2.class);
                invocation.proceed();
                journal.add(entry);
            }

            @Mock
            public void logBatchAlterJob(Invocation invocation, BatchAlterJobPersistInfo batchAlterJob,
                                         WALApplier walApplier) {
                List<String> entries = batchAlterJob.getAlterJobV2List().stream()
                        .map(alterJob -> GsonUtils.GSON.toJson(alterJob, AlterJobV2.class))
                        .collect(Collectors.toList());
                invocation.proceed();
                journal.addAll(entries);
            }
        };
        return journal;
    }

    /** The recorded journal entries of the job {@code jobId}, each one deserialized on its own. */
    private static List<AlterJobV2> journaledEntries(List<String> journal, long jobId) {
        return journal.stream()
                .map(entry -> GsonUtils.GSON.fromJson(entry, AlterJobV2.class))
                .filter(entry -> entry.getJobId() == jobId)
                .collect(Collectors.toList());
    }

    private static UtFrameUtils.PseudoImage saveMetastore() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(image.getImageWriter());
        return image;
    }

    private static LocalMetastore loadMetastore(UtFrameUtils.PseudoImage image) throws Exception {
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        metastore.load(reader);
        reader.close();
        return metastore;
    }

    /** Replays the journal entry into the rollup handler against the given catalog, as a follower does. */
    private static void replayOn(LocalMetastore metastore, MaterializedViewHandler handler, AlterJobV2 entry) {
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(metastore);
        try {
            handler.replayAlterJobV2(entry);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }
    }

    /**
     * The layout of a table: its state and the meta id of the rollup r, and the versions and the indexes of each
     * physical partition, with the state, tablets and tablet ranges of each index.
     */
    private static List<Object> layoutOf(OlapTable table) {
        List<Object> layout = new ArrayList<>();
        layout.add(table.getState());
        layout.add(table.getIndexMetaIdByName("r"));
        List<PhysicalPartition> partitions = new ArrayList<>(table.getPhysicalPartitions());
        partitions.sort(Comparator.comparingLong(PhysicalPartition::getId));
        for (PhysicalPartition partition : partitions) {
            layout.add(partition.getId());
            layout.add(partition.getVisibleVersion());
            layout.add(partition.getNextVersion());
            List<MaterializedIndex> indexes =
                    new ArrayList<>(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL));
            indexes.sort(Comparator.comparingLong(MaterializedIndex::getMetaId));
            for (MaterializedIndex index : indexes) {
                layout.add(index.getMetaId());
                layout.add(index.getState());
                for (Tablet tablet : index.getTablets()) {
                    layout.add(tablet.getId());
                    layout.add(tablet.getRange());
                }
            }
        }
        return layout;
    }

    /** The lower bounds of the tablets of {@code index} after the first one, which is unbounded below. */
    private static List<Tuple> lowerBoundsOf(MaterializedIndex index) {
        List<Tablet> tablets = index.getTablets();
        assertTrue(tablets.get(0).getRange().getRange().isMinimum());
        return tablets.subList(1, tablets.size()).stream()
                .map(tablet -> tablet.getRange().getRange().getLowerBound()).collect(Collectors.toList());
    }

    /**
     * A follower that replays the whole journal of the rollup job, and a frontend that recovers from an image taken at
     * WAITING_TXN and replays the rest of it, end with the layout the leader has after the flip: the same rollup
     * tablets with the same ranges, cut at the boundaries the leader planned.
     */
    @Test
    public void testRollupFollowerReplayAndWaitingTxnImageGiveLeaderLayout() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            List<String> journal = recordAlterJobJournal();
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_replay", TWO_PARTITIONS_BY_K2);
            UtFrameUtils.PseudoImage imageBeforeJob = saveMetastore();
            LakeRangeRollupJob job = submitRollup("t_rollup_replay", ROLLUP_K1_K3);
            job.setSampler(sixTabletSampler());
            mockRequestedTabletCount(6);
            job.runPendingJob();
            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            Map<Long, List<Tuple>> plannedBoundaries = new HashMap<>();
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                plannedBoundaries.put(partition.getId(), job.getBoundaries(partition.getId()));
                assertEquals(5, plannedBoundaries.get(partition.getId()).size());
            }
            UtFrameUtils.PseudoImage waitingTxnImage = saveMetastore();
            // An image holds each alter job as GSON.
            String waitingTxnJobImage = GsonUtils.GSON.toJson(job, AlterJobV2.class);
            int entriesInWaitingTxnImage = journaledEntries(journal, job.getJobId()).size();
            runWaitingTxnJobToRunning(job);
            runRunningJobToFinished(job);
            List<Object> leaderLayout = layoutOf(table);
            long rollupMetaId = job.getShadowIndexMetaId();

            // Each frontend deserializes the entries it replays: a replay installs objects of the entry in its catalog.
            LocalMetastore followerMetastore = loadMetastore(imageBeforeJob);
            MaterializedViewHandler followerHandler = new MaterializedViewHandler();
            for (AlterJobV2 entry : journaledEntries(journal, job.getJobId())) {
                replayOn(followerMetastore, followerHandler, entry);
            }
            OlapTable followerTable = (OlapTable) followerMetastore.getTable("test", "t_rollup_replay");

            LocalMetastore recoveredMetastore = loadMetastore(waitingTxnImage);
            MaterializedViewHandler recoveredHandler = new MaterializedViewHandler();
            recoveredHandler.addAlterJobV2(GsonUtils.GSON.fromJson(waitingTxnJobImage, AlterJobV2.class));
            List<AlterJobV2> entries = journaledEntries(journal, job.getJobId());
            for (AlterJobV2 entry : entries.subList(entriesInWaitingTxnImage, entries.size())) {
                replayOn(recoveredMetastore, recoveredHandler, entry);
            }
            OlapTable recoveredTable = (OlapTable) recoveredMetastore.getTable("test", "t_rollup_replay");

            for (OlapTable replayedTable : List.of(followerTable, recoveredTable)) {
                assertEquals(leaderLayout, layoutOf(replayedTable));
                RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(replayedTable);
                assertNotNull(dispatch);
                for (PhysicalPartition partition : replayedTable.getPhysicalPartitions()) {
                    MaterializedIndex rollup = partition.getLatestIndex(rollupMetaId);
                    assertEquals(plannedBoundaries.get(partition.getId()), lowerBoundsOf(rollup));
                    assertNotNull(dispatch.computeBucketSeq(rollup), "not aligned: partition " + partition.getId());
                }
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A new leader that takes the job over from the journal after the PACK placement re-runs the placement round: the
     * reconcile finds every tablet placed, moves none, and the job finishes rewriting.
     */
    @Test
    public void testRollupLeaderFailoverInRunningReplacesPackIdempotently() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_failover", "");
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            LakeRangeRollupJob job = submitRollup("t_rollup_failover", "r (k1, k3) order by (k1, k3)");
            job.setSampler(sixTabletSampler());
            mockRequestedTabletCount(6);
            runToRunning(job);
            markRewritesPublished(job);
            List<Boolean> reconcileResults = recordPackReconcileResults();
            job.runRunningJob();
            assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
            assertEquals(List.of(false), reconcileResults);
            MaterializedIndex shadowIndex = job.getShadowIndex(partition.getId());
            Map<Long, List<Long>> placedGroupIds = shardGroupIdsOf(shadowIndex);
            // What a new leader replays from the journal at this point.
            LakeRangeRollupJob takenOver = takenOverByNewLeader(job);
            assertEquals(AlterJobV2.JobState.RUNNING, takenOver.getJobState());

            takenOver.runRunningJob();

            assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, takenOver.getJobState());
            assertEquals(List.of(false, true), reconcileResults);
            assertEquals("", takenOver.errMsg);
            assertEquals(placedGroupIds, shardGroupIdsOf(shadowIndex));
            assertInPackGroupsOfColocateRanges(table, shadowIndex, rollupSortKey(table, job));
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A synchronous materialized view that keeps the colocate prefix runs to the flip like a rollup: its index is
     * aligned with the ColocateRanges in its own sort key and in their PACK shard groups, and the group stays stable.
     */
    @Test
    public void testSyncMvOnColocateTableRunsToFinishedAligned() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_sync_flip", "");
            createSyncMv("t_sync_flip", "mv_sync_flip", "select k1, k3, sum(v1) from t_sync_flip group by k1, k3");
            LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_sync_flip"));
            assertTrue(job.isSyncMaterializedView());
            job.setSampler(sixTabletSampler());
            mockRequestedTabletCount(6);

            runToRunning(job);
            runRunningJobToFinished(job);

            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Long mvMetaId = table.getIndexMetaIdByName("mv_sync_flip");
            assertEquals(job.getShadowIndexMetaId(), mvMetaId);
            assertEquals(List.of("k1", "k3"), columnNames(MetaUtils.getRangeDistributionColumns(table, mvMetaId)));
            assertEquals(6, table.getPhysicalPartitions().iterator().next().getLatestIndex(mvMetaId).getTablets().size());
            assertVisibleRollupAlignedAndInPack(table, mvMetaId);
            assertGroupStable(table);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * colocate_mv only selects the colocate load sender, which a range table never uses: on a member of a range
     * colocate group a synchronous materialized view that sets it is created, and its index meta does not keep it.
     */
    @Test
    public void testColocateMvPropertyIgnoredOnRangeColocateTable() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_sync_colocate_mv", "");
            CreateSyncMVStmt stmt = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                    "create materialized view mv_colocate_mv properties('colocate_mv' = 'true') as "
                            + "select k1, sum(v1) from t_sync_colocate_mv group by k1", connectContext);
            rollupHandler().processCreateMaterializedView(stmt, db(), table);
            LakeRangeRollupJob job = assertInstanceOf(LakeRangeRollupJob.class, soleRollupJob("t_sync_colocate_mv"));
            job.setSampler(stubSampler(List.of()));

            runToRunning(job);
            runRunningJobToFinished(job);

            Long mvMetaId = table.getIndexMetaIdByName("mv_colocate_mv");
            assertEquals(job.getShadowIndexMetaId(), mvMetaId);
            assertFalse(table.getIndexMetaByMetaId(mvMetaId).isColocateMVIndex());
            // The table is in a stable colocate group and has an index besides the base, so only the flag decides.
            assertFalse(table.getColocateGroup().isEmpty());
            assertEquals(2, table.getIndexMetaIdToMeta().size());
            assertGroupStable(table);
            assertFalse(table.isEnableColocateMVIndex());
            assertVisibleRollupAlignedAndInPack(table, mvMetaId);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /** Asks the frontend to create the automatic partition holding {@code value}, as a load would. */
    private static TCreatePartitionResult createAutomaticPartition(OlapTable table, String value) throws Exception {
        long loadTxnId = GlobalStateMgr.getCurrentState().getNextId();
        // The request names the load that needs the partition; any known transaction will do.
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long transactionId) {
                return transactionId == loadTxnId ? new TransactionState() : invocation.proceed();
            }
        };
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db().getId());
        request.setTable_id(table.getId());
        request.setTxn_id(loadTxnId);
        request.setPartition_values(List.of(List.of(value)));
        return new FrontendServiceImpl(ExecuteEnv.getInstance()).createPartition(request);
    }

    /**
     * ADD PARTITION waits for the rollup job: it is rejected while the job runs and, after the flip, creates the rollup
     * tablets aligned in the rollup's sort key. A load that needs a new automatic partition does not wait: it cancels
     * the job, and the partition is created aligned with the group.
     */
    @Test
    public void testAddPartitionDuringRollupJobRejectedAndAutomaticPartitionCancelsIt() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_addp", TWO_PARTITIONS_BY_K2);
            LakeRangeRollupJob job = submitRollup("t_rollup_addp", "r (k1, k3, k2) order by (k1, k3)");
            job.setSampler(stubSampler(List.of()));
            String addPartition = "alter table t_rollup_addp add partition p3 values less than ('300')";

            runToRunning(job);
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                assertAligned(table, job.getShadowIndex(partition.getId()), rollupSortKey(table, job));
            }
            AlterJobException e = assertThrows(AlterJobException.class, () -> starRocksAssert.alterTable(addPartition));
            assertInstanceOf(InvalidOlapTableStateException.class, e.getCause());
            assertTrue(e.getCause().getMessage().contains("A materialized view or rollup index is being created"),
                    e.getCause().getMessage());
            assertNull(table.getPartition("p3"));
            runRunningJobToFinished(job);

            starRocksAssert.alterTable(addPartition);

            long rollupMetaId = job.getShadowIndexMetaId();
            PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
            assertAligned(table, p3.getLatestBaseIndex(), MetaUtils.getRangeDistributionColumns(table));
            List<Column> rollupSortKey = MetaUtils.getRangeDistributionColumns(table, rollupMetaId);
            assertAligned(table, p3.getLatestIndex(rollupMetaId), rollupSortKey);
            assertInPackGroupsOfColocateRanges(table, p3.getLatestIndex(rollupMetaId), rollupSortKey);
            assertVisibleRollupAlignedAndInPack(table, rollupMetaId);

            OlapTable autoTable = createInThreeRangeGroup("t_rollup_autop", "k1 int, dt date, k2 int, v1 int",
                    "duplicate key(k1, dt, k2)\npartition by date_trunc('day', dt)", "k1, dt, k2");
            TCreatePartitionResult first = createAutomaticPartition(autoTable, "2026-01-01");
            assertEquals(TStatusCode.OK, first.getStatus().getStatus_code(), String.valueOf(first.getStatus()));
            LakeRangeRollupJob cancelledJob = submitRollup("t_rollup_autop", "r (k1, k2, dt) order by (k1, k2)");
            cancelledJob.setSampler(stubSampler(List.of()));
            runToRunning(cancelledJob);

            TCreatePartitionResult during = createAutomaticPartition(autoTable, "2026-01-02");

            assertEquals(TStatusCode.OK, during.getStatus().getStatus_code(), String.valueOf(during.getStatus()));
            assertEquals(AlterJobV2.JobState.CANCELLED, cancelledJob.getJobState());
            assertEquals(OlapTable.OlapTableState.NORMAL, autoTable.getState());
            assertNull(autoTable.getIndexMetaIdByName("r"));
            PhysicalPartition created = autoTable.getPartition("p20260102").getDefaultPhysicalPartition();
            assertNull(created.getLatestIndex(cancelledJob.getShadowIndexMetaId()));
            assertAligned(autoTable, created.getLatestBaseIndex(), MetaUtils.getRangeDistributionColumns(autoTable));
        } finally {
            rollupHandler().clearJobs();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Rollups through peer splits, several indexes, colocate joins and the operations around them
    // ---------------------------------------------------------------------------------------------

    /** Creates the peer table {@code <name>_peer} (k1, v2) in the colocate group {@code g_<name>:k1}. */
    private static OlapTable createPeer(String name) throws Exception {
        createColocateTable(name + "_peer", "k1 int, v2 int", "duplicate key(k1)", null, "g_" + name + ":k1");
        return table(name + "_peer");
    }

    /**
     * Answers the publish of a tablet split as a compute node does. A split at boundaries the frontend chose, such as an
     * alignment split of the colocate checker, gets those ranges back. A split that leaves the boundaries to the compute
     * node gets the ranges {@code chosenSplits} holds for its tablet, as if the compute node had chosen them from the data.
     */
    private static void answerSplitPublishes(Map<Long, List<TabletRange>> chosenSplits) {
        new MockUp<MockLakeService>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                return CompletableFuture.completedFuture(splitPublishResponse(List.of(request), chosenSplits));
            }

            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                return CompletableFuture.completedFuture(splitPublishResponse(request.publishReqs, chosenSplits));
            }
        };
    }

    private static PublishVersionResponse splitPublishResponse(List<PublishVersionRequest> requests,
                                                               Map<Long, List<TabletRange>> chosenSplits) {
        PublishVersionResponse response = new PublishVersionResponse();
        response.status = new StatusPB();
        response.status.statusCode = TStatusCode.OK.getValue();
        response.tabletRanges = new HashMap<>();
        for (PublishVersionRequest request : requests) {
            if (request.reshardingTabletInfos == null) {
                continue;
            }
            for (ReshardingTabletInfoPB info : request.reshardingTabletInfos) {
                if (info.splittingTabletInfo == null) {
                    continue;
                }
                List<Long> newTabletIds = info.splittingTabletInfo.newTabletIds;
                List<TabletRangePB> ranges = info.splittingTabletInfo.newTabletRanges;
                if (ranges == null || ranges.isEmpty()) {
                    ranges = chosenSplits.get(info.splittingTabletInfo.oldTabletId).stream()
                            .map(TabletRange::toProto).collect(Collectors.toList());
                }
                for (int i = 0; i < newTabletIds.size(); i++) {
                    response.tabletRanges.put(newTabletIds.get(i), ranges.get(i));
                }
            }
        }
        return response;
    }

    /** Runs a tablet reshard job to FINISHED. Its publish runs on the publish thread pool, so a round may find it pending. */
    private static void runReshardJobToFinished(TabletReshardJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (job.getJobState() != TabletReshardJob.JobState.FINISHED) {
            assertFalse(job.isDone(), "job " + job.getJobState() + ": " + job.getErrorMessage());
            assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
    }

    /**
     * Splits the tablet of {@code peer} in the ColocateRange [100, 200) at 150, submitted the way a SPLIT TABLET statement
     * is, and runs the job to FINISHED: the split adds the bound 150 to the ColocateRanges of the group and marks the
     * group unstable.
     */
    private static void splitPeerTabletAt150(OlapTable peer, Map<Long, List<TabletRange>> chosenSplits)
            throws Exception {
        ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(colocateRanges(peer),
                MetaUtils.getRangeDistributionColumns(peer), 1);
        Tablet tablet = peer.getPhysicalPartitions().iterator().next().getLatestBaseIndex().getTablets().stream()
                .filter(candidate -> classifier.indexOf(candidate) == 1).findFirst().orElseThrow();
        Range<Tuple> range = tablet.getRange().getRange();
        chosenSplits.put(tablet.getId(), List.of(new TabletRange(Range.gelt(range.getLowerBound(), intTuple(150))),
                new TabletRange(Range.gelt(intTuple(150), range.getUpperBound()))));
        // A negative target size names the number of new tablets, whatever the size of the tablet.
        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(List.of(tablet.getId())),
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2"));
        clause.setTabletReshardTargetSize(-2);
        runReshardJobToFinished(
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().createTabletReshardJob(db(), peer, clause));
    }

    private static List<TabletReshardJob> splitJobsOf(OlapTable table) {
        return GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().values().stream()
                .filter(job -> job.getJobType() == TabletReshardJob.JobType.SPLIT_TABLET)
                .filter(job -> job.getTableId() == table.getId())
                .collect(Collectors.toList());
    }

    /**
     * The bounds at which {@code split} cuts tablets of {@code partition}, by index meta id: the lower bound of every new
     * tablet but the first of each tablet it splits.
     */
    private static Map<Long, List<Tuple>> splitBoundsOf(TabletReshardJob split, PhysicalPartition partition) {
        Map<Long, List<Tuple>> bounds = new HashMap<>();
        for (ReshardingMaterializedIndex reshardingIndex : split.getReshardingPhysicalPartitions()
                .get(partition.getId()).getReshardingIndexes().values()) {
            List<Tuple> indexBounds = bounds.computeIfAbsent(reshardingIndex.getMaterializedIndex().getMetaId(),
                    metaId -> new ArrayList<>());
            for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                if (reshardingTablet.getSplittingTablet() == null) {
                    continue;
                }
                List<TabletRange> newRanges = reshardingTablet.getSplittingTablet().getNewTabletRanges();
                for (TabletRange newRange : newRanges.subList(1, newRanges.size())) {
                    indexBounds.add(newRange.getRange().getLowerBound());
                }
            }
        }
        return bounds;
    }

    /**
     * Asserts that every visible index of every partition, the base and each rollup, is aligned with the ColocateRanges
     * in its own sort key, has each tablet in the PACK shard group of its ColocateRange, and is bucketed by the colocate
     * scan dispatch.
     */
    private static void assertEveryIndexAlignedAndInPack(OlapTable table) throws Exception {
        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        assertNotNull(dispatch);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            for (MaterializedIndex index : partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table, index.getMetaId());
                assertAligned(table, index, sortKeyColumns);
                assertInPackGroupsOfColocateRanges(table, index, sortKeyColumns);
                assertNotNull(dispatch.computeBucketSeq(index),
                        "not aligned: partition " + partition.getId() + ", index " + index.getMetaId());
            }
        }
    }

    /** The meta id of the index the plan scans {@code table} with. */
    private static long scannedIndexMetaId(ExecPlan plan, OlapTable table) {
        List<Long> metaIds = plan.getScanNodes().stream()
                .filter(scanNode -> scanNode instanceof OlapScanNode
                        && scanNode.getDesc().getTable().getId() == table.getId())
                .map(scanNode -> ((OlapScanNode) scanNode).getSelectedIndexMetaId())
                .collect(Collectors.toList());
        assertEquals(1, metaIds.size(), "scans of " + table.getName());
        return metaIds.get(0);
    }

    /**
     * A split of a peer table that lands after the rollup was planned adds the ColocateRange bound 150 inside a rollup
     * tablet and marks the group unstable. While the table is in ROLLUP the colocate checker leaves it alone. The job
     * still finishes, and the rollup tablet that crosses the new bound is not placed into a PACK shard group. After the
     * flip the checker submits one alignment split of the table, which cuts the tablet across 150 of the base index and
     * of the rollup, each in its own sort key; once it is published the next cycle finds every index aligned and in PACK
     * and marks the group stable.
     */
    @Test
    public void testPeerSplitDuringRollupIsAlignedByColocateCheckerAfterFlip() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            Map<Long, List<TabletRange>> chosenSplits = new HashMap<>();
            answerSplitPublishes(chosenSplits);
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_peer_split", "");
            OlapTable peer = createPeer("t_rollup_peer_split");
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(table.getId());
            LakeRangeRollupJob job = submitRollup("t_rollup_peer_split", ROLLUP_K1_K3);
            job.setSampler(stubSampler(List.of()));
            job.runPendingJob();
            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            List<Column> rollupSortKey = rollupSortKey(table, job);
            assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), rollupSortKey);
            assertFalse(colocateTableIndex.isGroupUnstable(groupId));

            splitPeerTabletAt150(peer, chosenSplits);

            assertEquals(4, colocateRanges(table).size());
            assertTrue(colocateTableIndex.isGroupUnstable(groupId));
            ColocateChecker checker = new ColocateChecker();
            checker.runOneCycle();
            assertEquals(OlapTable.OlapTableState.ROLLUP, table.getState());
            assertTrue(colocateTableIndex.isGroupUnstable(groupId));
            assertEquals(List.of(), splitJobsOf(table));

            runWaitingTxnJobToRunning(job);
            runRunningJobToFinished(job);

            assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            long rollupMetaId = job.getShadowIndexMetaId();
            MaterializedIndex rollup = partition.getLatestIndex(rollupMetaId);
            List<ColocateRange> ranges = colocateRanges(table);
            ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(ranges, rollupSortKey, 1);
            Map<Long, List<Long>> groupIds = shardGroupIdsOf(rollup);
            List<Long> crossingTabletIds = new ArrayList<>();
            for (Tablet tablet : rollup.getTablets()) {
                int rangeIndex = classifier.indexOf(tablet);
                if (rangeIndex < 0) {
                    crossingTabletIds.add(tablet.getId());
                    assertEquals(List.of(rollup.getShardGroupId()), groupIds.get(tablet.getId()));
                } else {
                    assertEquals(Set.of(rollup.getShardGroupId(), ranges.get(rangeIndex).getShardGroupId()),
                            new HashSet<>(groupIds.get(tablet.getId())));
                }
            }
            assertEquals(1, crossingTabletIds.size());
            assertNull(RangeColocateScanDispatch.forTable(table).computeBucketSeq(rollup));
            assertTrue(colocateTableIndex.isGroupUnstable(groupId));

            checker.runOneCycle();

            List<TabletReshardJob> alignmentSplits = splitJobsOf(table);
            assertEquals(1, alignmentSplits.size());
            assertEquals(Map.of(table.getBaseIndexMetaId(), List.of(intTuple(150, null, null)),
                    rollupMetaId, List.of(intTuple(150, null))), splitBoundsOf(alignmentSplits.get(0), partition));
            assertTrue(colocateTableIndex.isGroupUnstable(groupId));

            runReshardJobToFinished(alignmentSplits.get(0));
            checker.runOneCycle();

            assertFalse(colocateTableIndex.isGroupUnstable(groupId));
            assertEquals(4, partition.getLatestIndex(rollupMetaId).getTablets().size());
            assertEquals(2, partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
            assertEveryIndexAlignedAndInPack(table);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * Creates the table {@code name} (k1, k2, k3, v1, dt), sorted by (k1, k2, k3) and partitioned by day on dt, in a group
     * with three ColocateRanges on k1, with the peer {@code <name>_peer}, and gives it two rollups of other lengths than
     * its sort key, added one after the other: r_short sorted by the colocate column k1 alone and r_long sorted by
     * (k1, k3, k2, v1), six tablets in each partition its job builds. A range rollup carries the partition column dt.
     * Then a partition is added by ADD PARTITION and another one is created for a load, so the table has three daily
     * partitions, besides the shadow partition every automatically partitioned table has.
     */
    private static OlapTable createTwoRollupTable(String name) throws Exception {
        OlapTable table = createInThreeRangeGroup(name, "k1 int, k2 int, k3 int, v1 int, dt date",
                "duplicate key(k1, k2, k3)\npartition by date_trunc('day', dt)", "k1, k2, k3");
        createPeer(name);
        TCreatePartitionResult first = createAutomaticPartition(table, "2026-01-01");
        assertEquals(TStatusCode.OK, first.getStatus().getStatus_code(), String.valueOf(first.getStatus()));
        mockRequestedTabletCount(6);
        LakeRangeRollupJob shortJob = submitRollup(name, "r_short (k1, dt) order by (k1)");
        shortJob.setSampler(stubSampler(List.of()));
        runToRunning(shortJob);
        runRunningJobToFinished(shortJob);
        LakeRangeRollupJob longJob = submitRollup(name, "r_long (k1, k3, k2, v1, dt) order by (k1, k3, k2, v1)");
        longJob.setSampler(stubSampler(List.of(
                intTuple(10, 1, 1, 1), intTuple(20, 1, 1, 1), intTuple(30, 1, 1, 1), intTuple(40, 1, 1, 1),
                intTuple(120, 5, 5, 5), intTuple(150, 1, 1, 1), intTuple(170, 1, 1, 1), intTuple(180, 1, 1, 1),
                intTuple(200, 1, 1, 1), intTuple(250, 1, 1, 1), intTuple(300, 1, 1, 1), intTuple(350, 1, 1, 1))));
        runToRunning(longJob);
        runRunningJobToFinished(longJob);
        assertEquals(List.of("k1"), columnNames(MetaUtils.getRangeDistributionColumns(table,
                table.getIndexMetaIdByName("r_short"))));
        assertEquals(List.of("k1", "k3", "k2", "v1"), columnNames(MetaUtils.getRangeDistributionColumns(table,
                table.getIndexMetaIdByName("r_long"))));
        assertEquals(6, table.getPartition("p20260101").getDefaultPhysicalPartition()
                .getLatestIndex(table.getIndexMetaIdByName("r_long")).getTablets().size());

        // An automatically partitioned table takes ADD PARTITION as a range of whole days.
        starRocksAssert.alterTable("alter table " + name
                + " add partitions start ('2026-01-02') end ('2026-01-03') every (interval 1 day)");
        TCreatePartitionResult automatic = createAutomaticPartition(table, "2026-01-03");
        assertEquals(TStatusCode.OK, automatic.getStatus().getStatus_code(), String.valueOf(automatic.getStatus()));
        for (String partitionName : List.of("p20260101", "p20260102", "p20260103")) {
            assertNotNull(table.getPartition(partitionName), partitionName);
        }
        return table;
    }

    /**
     * Splits the peer of {@code table} at 150 and lets the colocate checker align {@code table}: it submits one
     * alignment split, whose publish is answered as a compute node does, and the next cycle marks the group stable.
     * Returns the alignment split.
     */
    private static TabletReshardJob realignAfterPeerSplitAt150(OlapTable table) throws Exception {
        Map<Long, List<TabletRange>> chosenSplits = new HashMap<>();
        answerSplitPublishes(chosenSplits);
        splitPeerTabletAt150(table(table.getName() + "_peer"), chosenSplits);
        assertEquals(4, colocateRanges(table).size());
        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();
        List<TabletReshardJob> alignmentSplits = splitJobsOf(table);
        assertEquals(1, alignmentSplits.size());
        runReshardJobToFinished(alignmentSplits.get(0));
        checker.runOneCycle();
        assertGroupStable(table);
        return alignmentSplits.get(0);
    }

    /**
     * Two rollups of other lengths than the base sort key, one sorted by the colocate column alone and one longer than
     * the base, live through the life of the table with every index aligned in its own sort key and in PACK: their online
     * jobs, ADD PARTITION, a partition created for a load, and a split of the peer that adds a ColocateRange bound, which
     * the colocate checker aligns with one split that cuts the tablet across the bound of each index in its own sort key.
     */
    @Test
    public void testTwoRollupsOfDifferentLengthsStayAlignedThroughPartitionsAndChecker() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createTwoRollupTable("t_two_rollups");
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                assertEquals(3, partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
            }
            assertEveryIndexAlignedAndInPack(table);
            assertGroupStable(table);

            TabletReshardJob alignmentSplit = realignAfterPeerSplitAt150(table);

            Map<Long, List<Tuple>> boundOfEachIndex = Map.of(
                    table.getBaseIndexMetaId(), List.of(intTuple(150, null, null)),
                    table.getIndexMetaIdByName("r_short"), List.of(intTuple(150)),
                    table.getIndexMetaIdByName("r_long"), List.of(intTuple(150, null, null, null)));
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                assertEquals(boundOfEachIndex, splitBoundsOf(alignmentSplit, partition), "partition " + partition.getId());
            }
            assertEveryIndexAlignedAndInPack(table);
            assertGroupStable(table);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * On the table of the scenario above, after the checker aligned it with the bound added by the peer split, each
     * rollup is the index a query selects, and the colocate scan dispatch buckets it by the four ColocateRanges in
     * every partition.
     */
    @Test
    public void testEachRollupSelectedByAQueryHasABucketSeq() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createTwoRollupTable("t_two_rollups_seq");
            realignAfterPeerSplitAt150(table);
            RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
            assertNotNull(dispatch);
            String[][] cases = {
                    {"r_short", "select k1, dt from t_two_rollups_seq"},
                    {"r_long", "select v1 from t_two_rollups_seq where k1 = 150 and k3 = 1"}};
            for (String[] c : cases) {
                long rollupMetaId = table.getIndexMetaIdByName(c[0]);

                ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, c[1]).second;

                assertEquals(rollupMetaId, scannedIndexMetaId(plan, table), c[1]);
                for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                    Map<Long, Integer> bucketSeq = dispatch.computeBucketSeq(partition.getQueryableIndex(rollupMetaId));
                    assertNotNull(bucketSeq, c[0] + " of partition " + partition.getId());
                    assertEquals(Set.of(0, 1, 2, 3), new HashSet<>(bucketSeq.values()), c[0]);
                }
            }
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * CREATE TABLE joining a group with three ColocateRanges creates the tablets of each rollup with the ColocateRanges
     * expanded in the rollup's own key, here shorter and longer than the base sort key, and in the PACK shard groups of
     * their ColocateRanges; so does ADD PARTITION afterwards.
     */
    @Test
    public void testCreateTableRollupsOfOtherLengthsAreAlignedAndInPack() throws Exception {
        holdReshardDaemon();
        OlapTable member = createDupTableInThreeRangeGroup("t_ctr_lengths_member", "");
        starRocksAssert.withTable("create table t_ctr_lengths (k1 int, k2 int, k3 int, v1 int)\n"
                + "duplicate key(k1, k2, k3)\n"
                + "partition by range(k2) (partition p1 values less than ('100'))\n"
                + "order by(k1, k2, k3)\n"
                + "rollup (r_short (k1, k2, v1) duplicate key (k1),\n"
                + "        r_long (k1, k3, k2, v1) duplicate key (k1, k3, k2, v1))\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'g_t_ctr_lengths_member:k1');");

        OlapTable table = table("t_ctr_lengths");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        assertTrue(colocateTableIndex.isSameGroup(member.getId(), table.getId()));
        assertEquals(3, colocateRanges(table).size());
        assertEquals(List.of("k1", "k2", "k3"), columnNames(MetaUtils.getRangeDistributionColumns(table)));
        assertEquals(List.of("k1"), columnNames(MetaUtils.getRangeDistributionColumns(table,
                table.getIndexMetaIdByName("r_short"))));
        assertEquals(List.of("k1", "k3", "k2", "v1"), columnNames(MetaUtils.getRangeDistributionColumns(table,
                table.getIndexMetaIdByName("r_long"))));
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        assertEquals(3, p1.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEveryIndexAlignedAndInPack(table);

        starRocksAssert.alterTable("alter table t_ctr_lengths add partition p2 values less than ('200')");

        PhysicalPartition p2 = table.getPartition("p2").getDefaultPhysicalPartition();
        assertEquals(3, p2.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEveryIndexAlignedAndInPack(table);
        assertGroupStable(table);
    }

    /**
     * A join with a peer on the colocate column whose other columns are all in a rollup scans the rollup, is planned
     * colocated, and schedules: the dispatch buckets the six rollup tablets by the three ColocateRanges.
     */
    @Test
    public void testJoinReadingRollupIsPlannedColocate() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_join", "");
            createPeer("t_rollup_join");
            LakeRangeRollupJob job = submitRollup("t_rollup_join", "r (k1, k3, v1) order by (k1, k3)");
            job.setSampler(sixTabletSampler());
            mockRequestedTabletCount(6);
            runToRunning(job);
            runRunningJobToFinished(job);
            assertEquals(6, table.getPhysicalPartitions().iterator().next().getLatestIndex(job.getShadowIndexMetaId())
                    .getTablets().size());
            assertGroupStable(table);
            String join = "select t.k3, t.v1, p.v2 from t_rollup_join t join t_rollup_join_peer p on t.k1 = p.k1";

            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, join).second;

            String explain = plan.getExplainString(TExplainLevel.NORMAL);
            assertTrue(explain.contains("INNER JOIN (COLOCATE)"), explain);
            assertEquals(job.getShadowIndexMetaId(), scannedIndexMetaId(plan, table));
            UtFrameUtils.getPlanAndStartScheduling(connectContext, join);
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A sort-key rewrite rebuilds only the base index, so ALTER TABLE ... ORDER BY of a table with a rollup is not
     * routed to it and keeps the range-distribution rejection, also on a member of a range colocate group with a
     * rollup that keeps the colocate prefix.
     */
    @Test
    public void testSortKeyRewriteOfColocateTableWithRollupStaysRejected() throws Exception {
        holdReshardDaemon();
        starRocksAssert.withTable("create table t_reorder_with_rollup (k1 int, k2 int, k3 int, v1 int)\n"
                + "duplicate key(k1, k2, k3)\n"
                + "order by(k1, k2, k3)\n"
                + "rollup (r (k1, k3, v1))\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'g_reorder_with_rollup:k1');");
        OlapTable table = table("t_reorder_with_rollup");
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Set<Long> jobIdsBefore = new HashSet<>(schemaChangeHandler.getAlterJobsV2().keySet());

        AlterJobException e = assertThrows(AlterJobException.class,
                () -> starRocksAssert.alterTable("alter table t_reorder_with_rollup order by (k1, k3)"));

        assertEquals("Modifying sort key (ALTER TABLE ... ORDER BY) is not supported on tables with range distribution, "
                + "because the sort key defines tablet boundaries.", e.getMessage());
        assertEquals(jobIdsBefore, schemaChangeHandler.getAlterJobsV2().keySet());
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertEquals(List.of("k1", "k2", "k3"), columnNames(MetaUtils.getRangeDistributionColumns(table)));
    }

    /** ADD ROLLUP while a rollup job of the table runs is rejected by the table state, as before. */
    @Test
    public void testSecondRollupWhileFirstRunsRejected() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_rollup_second", "");
            LakeRangeRollupJob job = submitRollup("t_rollup_second", "r1 (k1, k3) order by (k1, k3)");
            job.setSampler(stubSampler(List.of()));
            runToRunning(job);

            AlterJobException e = assertThrows(AlterJobException.class,
                    () -> addRollup("t_rollup_second", "r2 (k1, v1) order by (k1)"));

            assertInstanceOf(InvalidOlapTableStateException.class, e.getCause());
            assertEquals(InvalidOlapTableStateException.of(OlapTable.OlapTableState.ROLLUP, "t_rollup_second")
                    .getMessage(), e.getCause().getMessage());
            assertEquals(List.of(job), rollupHandler().getUnfinishedAlterJobV2ByTableId(table.getId()));
            assertNull(table.getIndexMetaIdByName("r2"));
            assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
            assertEquals(OlapTable.OlapTableState.ROLLUP, table.getState());
        } finally {
            rollupHandler().clearJobs();
        }
    }

    /**
     * A rollup of a range table outside any colocate group is laid out as before: its boundaries are the row-quantile
     * cuts of the sample alone, its shards stay in the partition's shard group, and the job finishes rewriting without
     * any PACK placement round.
     */
    @Test
    public void testNonColocateRangeRollupLayoutUnchanged() throws Exception {
        holdReshardDaemon();
        holdRollupHandler();
        try {
            starRocksAssert.withTable("create table t_rollup_plain_layout (k1 int, k2 int, k3 int, v1 int)\n"
                    + "duplicate key(k1, k2, k3)\n"
                    + "order by(k1, k2, k3)\n"
                    + "properties('replication_num' = '1');");
            OlapTable table = table("t_rollup_plain_layout");
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            LakeRangeRollupJob job = submitRollup("t_rollup_plain_layout", "r (k1, k3) order by (k1, k3)");
            Sampler sampler = sixTabletSampler();
            job.setSampler(sampler);
            mockRequestedTabletCount(6);
            List<Boolean> reconcileResults = recordPackReconcileResults();

            job.runPendingJob();

            assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
            List<Tuple> sampledBoundaries = BoundaryPlanner.planRowQuantileBoundaries(sampler.sample(null), 6,
                    rollupSortKey(table, job)).getBoundaries();
            assertEquals(5, sampledBoundaries.size());
            assertEquals(sampledBoundaries, job.getBoundaries(partition.getId()));
            MaterializedIndex shadowIndex = job.getShadowIndex(partition.getId());
            long partitionShardGroupId = partition.getLatestBaseIndex().getShardGroupId();
            assertEquals(Set.of(List.of(partitionShardGroupId)), new HashSet<>(shardGroupIdsOf(shadowIndex).values()));
            runWaitingTxnJobToRunning(job);
            markRewritesPublished(job);

            job.runRunningJob();

            assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
            job.setPublishExecutor(publishingJob -> true);
            job.runFinishedRewritingJob();
            assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
            assertEquals(List.of(), reconcileResults);
            MaterializedIndex rollup = partition.getLatestIndex(job.getShadowIndexMetaId());
            assertEquals(6, rollup.getTablets().size());
            assertEquals(Set.of(List.of(partitionShardGroupId)), new HashSet<>(shardGroupIdsOf(rollup).values()));
        } finally {
            rollupHandler().clearJobs();
        }
    }
}
