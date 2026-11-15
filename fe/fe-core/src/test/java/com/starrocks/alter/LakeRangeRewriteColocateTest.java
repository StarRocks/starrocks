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
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.alter.reshard.presplit.Sampler;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
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
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStatusCode;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Range sort-key rewrites on shared-data tables and materialized views in a range colocate group: the new sort key must
 * start with the group's colocate columns, and a rejected statement must leave the dependent materialized views
 * untouched. Also covers the boundary planning against the group's ranges, the PACK placement of the shadow tablets,
 * the layout flip with its replay and the pinned colocate property, the materialized view path, and rewrites that
 * run alongside a peer split or a trailing key add.
 */
public class LakeRangeRewriteColocateTest {
    private static final String PREFIX_RULE_MESSAGE =
            "The sort key of a table in a range colocate group must start with its colocate columns";
    private static final String MV_PREFIX_RULE_MESSAGE =
            "The sort key of a materialized view in a range colocate group must start with its colocate columns";
    private static final String COLOCATE_COLUMN_K1_CHANGE_MESSAGE = "Can not modify colocate column[k1]";
    private static final String PIN_REJECTED_MESSAGE = "names only its colocate group and can not be written with an "
            + "explicit column list, so its sort key can not be made longer. Recreate the ";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // The tests inspect the jobs analyzeAndCreateJob builds; nothing may run them in the background.
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

    private static SchemaChangeHandler handler() {
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
    }

    private static Database db() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
    }

    /**
     * Creates a range table in the colocate group {@code colocateWith} ({@code g} or {@code g:<columns>}).
     * {@code orderBy} may be null for a sort key derived from the key columns.
     */
    private static void createColocateTable(String name, String columns, String keys, String orderBy,
                                            String colocateWith) throws Exception {
        starRocksAssert.withTable("create table " + name + " (" + columns + ")\n"
                + keys + "\n"
                + (orderBy == null ? "" : "order by(" + orderBy + ")\n")
                + "properties('replication_num' = '1', 'colocate_with' = '" + colocateWith + "');");
    }

    /** DUP table (k1, k2, k3, v1) sorted by (k1, k2, k3). */
    private static void createDupColocateTable(String name, String colocateWith) throws Exception {
        createColocateTable(name, "k1 int NOT NULL, k2 int NOT NULL, k3 int NOT NULL, v1 int",
                "duplicate key(k1, k2, k3)", "k1, k2, k3", colocateWith);
    }

    /** DUP table (k1, k2, v1) whose sort key is derived from its key columns (k1, k2). */
    private static void createKeyDerivedColocateTable(String name, String colocateWith) throws Exception {
        createColocateTable(name, "k1 int NOT NULL, k2 int NOT NULL, v1 int",
                "duplicate key(k1, k2)", null, colocateWith);
    }

    /** Runs analyzeAndCreateJob, the way the ALTER executor does, and returns the job it builds. */
    private static AlterJobV2 createJob(String tableName, String alterSql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        return handler().analyzeAndCreateJob(stmt.getAlterClauseList(), db(), table(tableName));
    }

    /**
     * Asserts that {@code executable} fails with an error containing {@code expectedMessage} and returns the
     * message. analyzeAndCreateJob throws a DdlException; a full ALTER statement rethrows its message as an
     * AlterJobException.
     */
    private static String assertRejected(Executable executable, String expectedMessage) {
        Throwable e = assertThrows(Throwable.class, executable);
        assertTrue(String.valueOf(e.getMessage()).contains(expectedMessage), "Unexpected error: " + e);
        return e.getMessage();
    }

    private static String assertPrefixRuleRejected(Executable executable) {
        return assertRejected(executable, PREFIX_RULE_MESSAGE);
    }

    /** Runs {@code alterSql} as a full ALTER TABLE statement and checks it is rejected without registering a job. */
    private static void assertFullAlterRejected(String alterSql, String expectedMessage) {
        Set<Long> jobIdsBefore = new HashSet<>(handler().getAlterJobsV2().keySet());
        assertRejected(() -> starRocksAssert.alterTable(alterSql), expectedMessage);
        assertEquals(jobIdsBefore, handler().getAlterJobsV2().keySet(), "no alter job may be registered");
    }

    /**
     * Creates an async materialized view over {@code tableName}, then checks that the full ALTER TABLE
     * {@code alterSql} is rejected before anything changes: no alter job is registered, and the view keeps its
     * active state, its refresh task state and its version map.
     */
    private static void assertFullAlterRejectedKeepingMvActive(String tableName, String alterSql,
                                                               String expectedMessage) throws Exception {
        String mvName = "mv_" + tableName;
        starRocksAssert.withMaterializedView("create materialized view " + mvName + "\n"
                + "distributed by hash(k1) buckets 1\n"
                + "refresh deferred manual\n"
                + "as select k1, k2 from " + tableName);
        MaterializedView mv = (MaterializedView) table(mvName);
        assertTrue(mv.isActive());
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(mv);
        assertNotNull(task);
        Constants.TaskState taskStateBefore = task.getState();
        // Seed the version map: inactivating the view would clear it.
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Map<String, MaterializedView.BasePartitionInfo> partitionVersions = new HashMap<>();
        partitionVersions.put(tableName, new MaterializedView.BasePartitionInfo(1L, 2L, 3L));
        versionMap.put(table(tableName).getId(), partitionVersions);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMapBefore = new HashMap<>(versionMap);

        assertFullAlterRejected(alterSql, expectedMessage);

        assertTrue(mv.isActive(), "the dependent view must stay active, reason: " + mv.getInactiveReason());
        assertEquals(taskStateBefore, GlobalStateMgr.getCurrentState().getTaskManager().getTask(mv).getState());
        assertEquals(versionMapBefore, versionMap);
    }

    // ---------------------------------------------------------------------------------------------
    // ALTER TABLE ... ORDER BY
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testOrderByKeepingColocatePrefixCreatesRewriteJob() throws Exception {
        createDupColocateTable("t_colo_keep", "g_colo_keep:k1");
        AlterJobV2 job = createJob("t_colo_keep", "alter table t_colo_keep order by (k1, k3, k2)");
        assertInstanceOf(LakeRangeRewriteSchemaChangeJob.class, job);
    }

    @Test
    public void testOrderByDroppingColocatePrefixRejected() throws Exception {
        createDupColocateTable("t_colo_drop_prefix", "g_colo_drop_prefix:k1");
        String message = assertPrefixRuleRejected(
                () -> createJob("t_colo_drop_prefix", "alter table t_colo_drop_prefix order by (k2, k3)"));
        assertEquals("The sort key of a table in a range colocate group must start with its colocate columns [k1] "
                + "(same order and types). Got: [k2, k3]", message);
    }

    @Test
    public void testOrderByReorderingColocatePrefixRejected() throws Exception {
        createDupColocateTable("t_colo_reorder", "g_colo_reorder:k1,k2");
        assertPrefixRuleRejected(() -> createJob("t_colo_reorder", "alter table t_colo_reorder order by (k2, k1, k3)"));
    }

    /**
     * Columns of a table created before column unique ids were assigned all carry the same unset id, so the rule
     * must fall back to names: otherwise swapping two colocate columns of the same type would pass.
     */
    @Test
    public void testOrderBySwappingColocateColumnsWithoutUniqueIdsRejected() throws Exception {
        createDupColocateTable("t_colo_no_uid", "g_colo_no_uid:k1,k2");
        for (Column column : table("t_colo_no_uid").getBaseSchema()) {
            column.setUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);
        }
        assertPrefixRuleRejected(() -> createJob("t_colo_no_uid", "alter table t_colo_no_uid order by (k2, k1, k3)"));
    }

    @Test
    public void testPrimaryKeyNonKeyAutoIncrementStillRejected() throws Exception {
        createColocateTable("t_colo_pk_autoinc",
                "k1 int NOT NULL, k2 int NOT NULL, id bigint NOT NULL auto_increment, v1 int",
                "primary key(k1, k2)", "k1, k2", "g_colo_pk_autoinc:k1");
        DdlException e = assertThrows(DdlException.class,
                () -> createJob("t_colo_pk_autoinc", "alter table t_colo_pk_autoinc order by (k1, k2)"));
        assertTrue(e.getMessage().contains("PRIMARY KEY table with a non-key AUTO_INCREMENT column"),
                "Unexpected message: " + e.getMessage());
    }

    // ---------------------------------------------------------------------------------------------
    // MODIFY COLUMN integer widen, DROP COLUMN, ADD key column
    // ---------------------------------------------------------------------------------------------

    /**
     * A widen of a colocate column never reaches the rewrite builder: the colocate column check of the schema
     * change analysis rejects it first, with its own message.
     */
    @Test
    public void testIntegerWidenOfColocateColumnRejected() throws Exception {
        createDupColocateTable("t_colo_widen", "g_colo_widen:k1");
        assertFullAlterRejected("alter table t_colo_widen modify column k1 bigint key NOT NULL",
                COLOCATE_COLUMN_K1_CHANGE_MESSAGE);
    }

    @Test
    public void testIntegerWidenOutsideColocatePrefixCreatesRewriteJob() throws Exception {
        createDupColocateTable("t_colo_widen_tail", "g_colo_widen_tail:k1");
        AlterJobV2 job = createJob("t_colo_widen_tail",
                "alter table t_colo_widen_tail modify column k2 bigint key NOT NULL");
        assertInstanceOf(LakeRangeRewriteSchemaChangeJob.class, job);
    }

    @Test
    public void testDropColocateColumnRejected() throws Exception {
        createDupColocateTable("t_colo_drop", "g_colo_drop:k1");
        assertPrefixRuleRejected(() -> createJob("t_colo_drop", "alter table t_colo_drop drop column k1"));
    }

    @Test
    public void testAddKeyColumnInFrontOfColocatePrefixRejected() throws Exception {
        createKeyDerivedColocateTable("t_colo_add_front", "g_colo_add_front:k1");
        assertPrefixRuleRejected(() -> createJob("t_colo_add_front",
                "alter table t_colo_add_front add column k0 int key default '0' first"));
    }

    @Test
    public void testTrailingKeyAddCreatesMetadataOnlyJob() throws Exception {
        createKeyDerivedColocateTable("t_colo_add_tail", "g_colo_add_tail:k1");
        AlterJobV2 job = createJob("t_colo_add_tail", "alter table t_colo_add_tail add column k3 int key default '0'");
        assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
        assertFalse(job instanceof LakeRangeRewriteSchemaChangeJob, "must not rewrite the data, got: " + job);
        LakeTableAsyncFastSchemaChangeJob metadataOnlyJob = (LakeTableAsyncFastSchemaChangeJob) job;
        assertNotNull(metadataOnlyJob.getTargetRanges());
        assertFalse(metadataOnlyJob.getTargetRanges().isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // A rejected rewrite leaves dependent materialized views active
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testRejectedOrderByKeepsDependentMvActive() throws Exception {
        createDupColocateTable("t_colo_mv_order", "g_colo_mv_order:k1");
        assertFullAlterRejectedKeepingMvActive("t_colo_mv_order", "alter table t_colo_mv_order order by (k2, k1)",
                PREFIX_RULE_MESSAGE);
    }

    @Test
    public void testRejectedDropColumnKeepsDependentMvActive() throws Exception {
        createDupColocateTable("t_colo_mv_drop", "g_colo_mv_drop:k1");
        assertFullAlterRejectedKeepingMvActive("t_colo_mv_drop", "alter table t_colo_mv_drop drop column k1",
                PREFIX_RULE_MESSAGE);
    }

    @Test
    public void testRejectedKeynessFlipKeepsDependentMvActive() throws Exception {
        // Demoting k2 leaves the key-derived sort key (k1), shorter than the two colocate columns.
        createKeyDerivedColocateTable("t_colo_mv_flip", "g_colo_mv_flip:k1,k2");
        assertFullAlterRejectedKeepingMvActive("t_colo_mv_flip",
                "alter table t_colo_mv_flip modify column k2 int NOT NULL", PREFIX_RULE_MESSAGE);
    }

    @Test
    public void testRejectedIntegerWidenKeepsDependentMvActive() throws Exception {
        createDupColocateTable("t_colo_mv_widen", "g_colo_mv_widen:k1");
        assertFullAlterRejectedKeepingMvActive("t_colo_mv_widen",
                "alter table t_colo_mv_widen modify column k1 bigint key NOT NULL", COLOCATE_COLUMN_K1_CHANGE_MESSAGE);
    }

    // ---------------------------------------------------------------------------------------------
    // Shadow tablet boundaries of a table in a group with three ColocateRanges
    // ---------------------------------------------------------------------------------------------

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

    private static LakeRangeRewriteSchemaChangeJob createRewriteJob(String tableName, String alterSql) throws Exception {
        return (LakeRangeRewriteSchemaChangeJob) createJob(tableName, alterSql);
    }

    /** The one unfinished alter job of {@code tableName}. */
    private static AlterJobV2 soleUnfinishedJob(String tableName) {
        List<AlterJobV2> jobs = handler().getUnfinishedAlterJobV2ByTableId(table(tableName).getId());
        assertEquals(1, jobs.size());
        return jobs.get(0);
    }

    /** Runs the full ALTER TABLE statement and returns the rewrite job it registered; no background handler runs it. */
    private static LakeRangeRewriteSchemaChangeJob submitRewrite(String tableName, String alterSql) throws Exception {
        starRocksAssert.alterTable(alterSql);
        return (LakeRangeRewriteSchemaChangeJob) soleUnfinishedJob(tableName);
    }

    /** A sampler returning canned tuples of the new sort key. */
    private static Sampler stubSampler(List<Tuple> tuples) {
        return request -> tuples.isEmpty() ? SampleSet.EMPTY : new SampleSet(tuples, new Estimates(1024L, tuples.size()));
    }

    /** Makes the rewrite ask for {@code tabletCount} tablets per partition, whatever the size of the partition. */
    private static void mockRequestedTabletCount(int tabletCount) {
        new MockUp<LakeOnlineRewriteJobBase>() {
            @Mock
            int selectRequestedTabletCount(LakeOnlineRewriteJobBase.PendingPartitionPlan plan, int activeComputeNodeCount,
                                           long targetSize) {
                return tabletCount;
            }
        };
    }

    /** Keeps the reshard daemon, and the colocate checker it runs, from acting on the tables of a test. */
    private static void holdReshardDaemon() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
    }

    /** Records the shard group ids of every StarOS shard created for the index {@code indexMetaId}. */
    private static List<List<Long>> recordShardGroupIdsOfIndex(long indexMetaId) {
        List<List<Long>> recorded = new ArrayList<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, long metaGroupId,
                                           ComputeResource computeResource) {
                if (String.valueOf(indexMetaId).equals(properties.get(LakeTablet.PROPERTY_KEY_INDEX_ID))) {
                    recorded.add(List.copyOf(groupIds));
                }
                return invocation.proceed();
            }
        };
        return recorded;
    }

    private static void assertAligned(OlapTable table, MaterializedIndex index, List<Column> sortKeyColumns) {
        assertTrue(RangeColocateScanDispatch.isTabletRangesAligned(index, sortKeyColumns, colocateRanges(table), 1),
                "not aligned with the colocate ranges: " + index.getTablets().stream()
                        .map(tablet -> String.valueOf(tablet.getRange().getRange())).collect(Collectors.toList()));
    }

    /** Asserts that {@code index} has exactly one tablet inside each of the three ColocateRanges of the table's group. */
    private static void assertOneTabletPerColocateRange(OlapTable table, MaterializedIndex index,
                                                        List<Column> sortKeyColumns) {
        ColocateRangeUtils.Classifier classifier =
                ColocateRangeUtils.Classifier.of(colocateRanges(table), sortKeyColumns, 1);
        assertEquals(List.of(0, 1, 2), index.getTablets().stream()
                .map(tablet -> classifier.indexOf(tablet)).sorted().collect(Collectors.toList()));
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

    /** Asserts that StarOS has every tablet of {@code index} in the PACK shard group of the ColocateRange it lies in. */
    private static void assertInPackGroupsOfColocateRanges(OlapTable table, MaterializedIndex index,
                                                           List<Column> sortKeyColumns) throws Exception {
        List<ColocateRange> ranges = colocateRanges(table);
        ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(ranges, sortKeyColumns, 1);
        Map<Long, List<Long>> groupIds = shardGroupIdsOf(index);
        for (Tablet tablet : index.getTablets()) {
            long packGroupId = ranges.get(classifier.indexOf(tablet)).getShardGroupId();
            assertTrue(groupIds.get(tablet.getId()).contains(packGroupId), "tablet " + tablet.getId() + " "
                    + tablet.getRange().getRange() + " is in " + groupIds.get(tablet.getId()) + ", not in " + packGroupId);
        }
    }

    /** Runs the job from PENDING to RUNNING: the loads before the watershed are reported finished. */
    private static void runToRunning(LakeRangeRewriteSchemaChangeJob job) throws Exception {
        job.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        runWaitingTxnJobToRunning(job);
    }

    /** Runs a WAITING_TXN job to RUNNING: the loads before the watershed are reported finished. */
    private static void runWaitingTxnJobToRunning(LakeRangeRewriteSchemaChangeJob job) throws Exception {
        // The first round allocates the watershed and exposes the shadow index to loads.
        job.runWaitingTxnJob();
        new MockUp<LakeRangeRewriteSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return true;
            }
        };
        job.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
    }

    /** Reports the rewrite of each partition of a RUNNING job published, as the transaction of its INSERT would be. */
    private static void markRewritesPublished(LakeRangeRewriteSchemaChangeJob job) {
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
    private static void runRunningJobToFinished(LakeRangeRewriteSchemaChangeJob job) throws Exception {
        markRewritesPublished(job);
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        job.setPublishExecutor(publishingJob -> true);
        job.runFinishedRewritingJob();
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
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
     * The shadow boundaries are the ColocateRange bounds expanded into the new sort key plus row-quantile cuts of the
     * sample inside each ColocateRange. A cut whose values after the colocate column are all NULL is dropped, and a
     * sample equal to a ColocateRange bound does not add a second boundary. The shards of the shadow tablets are
     * created in the partition's shard group only, not in the PACK shard groups of their ColocateRanges.
     */
    @Test
    public void testShadowBoundariesUnionColocateBoundsAndSampledCuts() throws Exception {
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_plan", "");
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_plan", "alter table t_colo_plan order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of(
                // (-inf, 100)
                intTuple(10, 1, 1), intTuple(20, 1, 1), intTuple(30, 1, 1), intTuple(40, 1, 1),
                // [100, 200), with the expanded bound (100, NULL, NULL) and the canonical-shaped (150, NULL, NULL)
                intTuple(100, null, null), intTuple(120, 5, 5), intTuple(150, null, null), intTuple(150, 1, 1),
                intTuple(170, 1, 1), intTuple(180, 1, 1), intTuple(190, 1, 1), intTuple(199, 9, 9),
                // [200, +inf)
                intTuple(200, 1, 1), intTuple(250, 1, 1), intTuple(300, 1, 1), intTuple(350, 1, 1))));
        mockRequestedTabletCount(6);
        List<List<Long>> shadowShardGroupIds = recordShardGroupIdsOfIndex(job.getShadowIndexMetaId());

        job.runPendingJob();

        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        // The six requested tablets are shared 1:2:1 by sample size, so the ranges ask for 2, 3 and 2 tablets; the
        // middle range loses its canonical-shaped cut (150, NULL, NULL).
        assertEquals(List.of(intTuple(30, 1, 1), intTuple(100, null, null), intTuple(180, 1, 1),
                intTuple(200, null, null), intTuple(300, 1, 1)), job.getBoundaries(partition.getId()));
        assertAligned(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
        assertEquals(List.of(List.of(partition.getLatestBaseIndex().getShardGroupId())), shadowShardGroupIds);
    }

    @Test
    public void testSamplingFailureGivesOneShadowTabletPerColocateRange() throws Exception {
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_sample_fail", "");
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_sample_fail", "alter table t_colo_sample_fail order by (k1, k3, k2)");
        job.setSampler(request -> {
            throw new StarRocksException("sampling failed");
        });
        mockRequestedTabletCount(6);

        job.runPendingJob();

        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        assertEquals(List.of(intTuple(100, null, null), intTuple(200, null, null)), job.getBoundaries(partition.getId()));
        assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
    }

    /** When the new sort key is the colocate columns, every cut would have the shape of a ColocateRange bound. */
    @Test
    public void testSortKeyOfColocateColumnsGivesOneShadowTabletPerColocateRangeWithoutSampling() throws Exception {
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_whole_key", "");
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_whole_key", "alter table t_colo_whole_key order by (k1)");
        job.setSampler(request -> {
            throw new AssertionError("no sample can split a ColocateRange of a sort key made of the colocate columns");
        });
        mockRequestedTabletCount(6);

        job.runPendingJob();

        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        assertEquals(List.of(intTuple(100), intTuple(200)), job.getBoundaries(partition.getId()));
        assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
    }

    /** With one tablet requested, no ColocateRange can get a second one, so the partition is not sampled. */
    @Test
    public void testOneRequestedTabletGivesOneShadowTabletPerColocateRangeWithoutSampling() throws Exception {
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_one_tablet", "");
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_one_tablet", "alter table t_colo_one_tablet order by (k1, k3, k2)");
        job.setSampler(request -> {
            throw new AssertionError("no sample can add a tablet when one tablet is requested");
        });
        mockRequestedTabletCount(1);

        job.runPendingJob();

        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        assertEquals(List.of(intTuple(100, null, null), intTuple(200, null, null)), job.getBoundaries(partition.getId()));
        assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
    }

    /** With automatic resharding off, each ColocateRange keeps the number of base tablets it has. */
    @Test
    public void testPreserveModeKeepsTabletCountOfEachColocateRange() throws Exception {
        long savedTargetSize = Config.tablet_reshard_target_size;
        Config.tablet_reshard_target_size = 0;
        try {
            OlapTable table = createDupTableInThreeRangeGroup("t_colo_preserve", "");
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
            LakeRangeRewriteSchemaChangeJob job =
                    createRewriteJob("t_colo_preserve", "alter table t_colo_preserve order by (k1, k3, k2)");
            job.setSampler(stubSampler(List.of(intTuple(10, 1, 1), intTuple(20, 1, 1),
                    intTuple(150, 1, 1), intTuple(160, 1, 1), intTuple(170, 1, 1), intTuple(180, 1, 1),
                    intTuple(210, 1, 1), intTuple(220, 1, 1))));

            job.runPendingJob();

            assertEquals(List.of(intTuple(100, null, null), intTuple(170, 1, 1), intTuple(200, null, null)),
                    job.getBoundaries(partition.getId()));
            assertAligned(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
        } finally {
            Config.tablet_reshard_target_size = savedTargetSize;
        }
    }

    /** Makes a job built on a table (k1, k2, k3, ...) look as if it had been built with the new sort key (k3, k1, k2). */
    private static void setNewSortKeyK3K1K2(LakeRangeRewriteSchemaChangeJob job) {
        List<Column> newSchema = job.getNewSchema();
        job.setNewSortKeyIdxes(List.of(2, 0, 1));
        job.setNewSortKeyUniqueIds(List.of(newSchema.get(2).getUniqueId(), newSchema.get(0).getUniqueId(),
                newSchema.get(1).getUniqueId()));
        job.setNewSortKeyColumns(List.of(newSchema.get(2), newSchema.get(0), newSchema.get(1)));
    }

    /** The colocate prefix rule is checked again under the table lock before the shadow index is installed. */
    @Test
    public void testPendingJobCancelledWhenNewSortKeyBreaksColocatePrefix() throws Exception {
        createDupColocateTable("t_colo_recheck", "g_colo_recheck:k1");
        OlapTable table = table("t_colo_recheck");
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_recheck", "alter table t_colo_recheck order by (k1, k3, k2)");
        setNewSortKeyK3K1K2(job);
        job.setSampler(stubSampler(List.of()));

        AlterCancelException e = assertThrows(AlterCancelException.class, job::runPendingJob);

        assertTrue(e.getMessage().contains(PREFIX_RULE_MESSAGE), e.getMessage());
        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()));
    }

    /**
     * An empty partition has no sample, so its shadow index gets one tablet per ColocateRange. The job runs to the
     * flip; a load committed after the watershed writes the shadow tablets and is read from them after the flip, and
     * every new tablet is in the PACK shard group of its ColocateRange.
     */
    @Test
    public void testEmptyPartitionRewriteRunsToFinishedWithOneTabletPerColocateRange() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_empty", "");
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_colo_empty", "alter table t_colo_empty order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));

        runToRunning(job);
        MaterializedIndex shadowIndex = job.getShadowIndex(partition.getId());
        assertOneTabletPerColocateRange(table, shadowIndex, job.getNewSortKeyColumns());
        // A load after the watershed writes the base index and the shadow index.
        ConcurrentLoad.holdPublishDaemon();
        TransactionState load = ConcurrentLoad.commit(db(), table);
        ConcurrentLoad.makeVisible(db(), load);
        assertTrue(shadowIndex.visibleForTransaction(load.getTransactionId()));
        long loadVersion = ConcurrentLoad.versionIn(load, table, partition.getId());
        runRunningJobToFinished(job);

        // After the flip the shadow index is the base index, aligned with the group.
        MaterializedIndex newBaseIndex = partition.getLatestBaseIndex();
        assertEquals(job.getShadowIndexMetaId(), newBaseIndex.getMetaId());
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        assertAligned(table, newBaseIndex, sortKeyColumns);
        assertOneTabletPerColocateRange(table, newBaseIndex, sortKeyColumns);
        assertInPackGroupsOfColocateRanges(table, newBaseIndex, sortKeyColumns);
        // A query reads the load from the new tablets.
        OlapScanNode scanNode = (OlapScanNode) UtFrameUtils.getPlanAndFragment(connectContext,
                "select * from t_colo_empty").second.getScanNodes().get(0);
        Set<Long> scannedTabletIds = new HashSet<>();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            TInternalScanRange scanRange = locations.getScan_range().getInternal_scan_range();
            scannedTabletIds.add(scanRange.getTablet_id());
            assertTrue(Long.parseLong(scanRange.getVersion()) >= loadVersion, scanRange.getVersion());
        }
        assertEquals(newBaseIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toSet()),
                scannedTabletIds);
    }

    /** ADD PARTITION waits for the rewrite: the new partition must be created with the new sort key. */
    @Test
    public void testAddPartitionRejectedDuringRewriteAndAcceptedAfterFlip() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_addp", "\npartition by range(k2) "
                + "(partition p1 values less than ('100'), partition p2 values less than ('200'))");
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_colo_addp", "alter table t_colo_addp order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));
        String addPartition = "alter table t_colo_addp add partition p3 values less than ('300')";

        runToRunning(job);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            assertAligned(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
        }
        AlterJobException e = assertThrows(AlterJobException.class, () -> starRocksAssert.alterTable(addPartition));
        assertInstanceOf(InvalidOlapTableStateException.class, e.getCause());
        assertTrue(e.getCause().getMessage().contains("A schema change operation is in progress"),
                e.getCause().getMessage());
        assertNull(table.getPartition("p3"));
        runRunningJobToFinished(job);

        starRocksAssert.alterTable(addPartition);
        assertAligned(table, table.getPartition("p3").getDefaultPhysicalPartition().getLatestBaseIndex(),
                MetaUtils.getRangeDistributionColumns(table));
    }

    /**
     * A load that needs a new automatic partition does not wait for the rewrite: it cancels the job, as it does any
     * schema change that does not tolerate concurrent partition creation, and the partition is created with the
     * current sort key. Once a rewrite has flipped, the partition is created with the new sort key. Either way the
     * partition is aligned with the group.
     */
    @Test
    public void testAutomaticPartitionCancelsRunningRewriteAndIsAlignedAfterFlip() throws Exception {
        holdReshardDaemon();
        OlapTable table = createInThreeRangeGroup("t_colo_autop", "k1 int, dt date, k2 int, v1 int",
                "duplicate key(k1, dt, k2)\npartition by date_trunc('day', dt)", "k1, dt, k2");
        TCreatePartitionResult first = createAutomaticPartition(table, "2026-01-01");
        assertEquals(TStatusCode.OK, first.getStatus().getStatus_code(), String.valueOf(first.getStatus()));
        String orderBy = "alter table t_colo_autop order by (k1, k2, dt)";
        LakeRangeRewriteSchemaChangeJob cancelledJob = submitRewrite("t_colo_autop", orderBy);
        cancelledJob.setSampler(stubSampler(List.of()));

        runToRunning(cancelledJob);
        TCreatePartitionResult during = createAutomaticPartition(table, "2026-01-02");
        assertEquals(TStatusCode.OK, during.getStatus().getStatus_code(), String.valueOf(during.getStatus()));
        assertEquals(AlterJobV2.JobState.CANCELLED, cancelledJob.getJobState());
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertAligned(table, table.getPartition("p20260102").getDefaultPhysicalPartition().getLatestBaseIndex(),
                MetaUtils.getRangeDistributionColumns(table));

        LakeRangeRewriteSchemaChangeJob job = submitRewrite("t_colo_autop", orderBy);
        job.setSampler(stubSampler(List.of()));
        runToRunning(job);
        runRunningJobToFinished(job);

        TCreatePartitionResult after = createAutomaticPartition(table, "2026-01-03");
        assertEquals(TStatusCode.OK, after.getStatus().getStatus_code(), String.valueOf(after.getStatus()));
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        assertEquals(List.of("k1", "k2", "dt"), sortKeyColumns.stream().map(Column::getName).collect(Collectors.toList()));
        assertAligned(table, table.getPartition("p20260103").getDefaultPhysicalPartition().getLatestBaseIndex(),
                sortKeyColumns);
    }

    /** A shadow index that is not aligned with the ColocateRanges is never installed. */
    @Test
    public void testPendingJobCancelledWhenShadowIsNotAligned() throws Exception {
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_unaligned", "");
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_unaligned", "alter table t_colo_unaligned order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of(intTuple(150, 1, 1), intTuple(160, 1, 1))));
        mockRequestedTabletCount(6);
        // A planning defect that loses the ColocateRange bounds.
        new MockUp<RangeColocateRewriteLayout>() {
            @Mock
            List<Tuple> planBoundaries(List<List<Tuple>> samplesByRange, int[] tabletCounts) {
                return List.of(intTuple(160, 1, 1));
            }
        };

        AlterCancelException e = assertThrows(AlterCancelException.class, job::runPendingJob);

        assertTrue(e.getMessage().contains("is not aligned with the colocate ranges"), e.getMessage());
        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertNull(table.getIndexMetaByMetaId(job.getShadowIndexMetaId()));
    }

    @Test
    public void testPendingJobCancelledWhenColocateRangesAreMissing() throws Exception {
        createDupColocateTable("t_colo_no_ranges", "g_colo_no_ranges:k1");
        OlapTable table = table("t_colo_no_ranges");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        colocateTableIndex.getColocateRangeMgr()
                .setColocateRanges(colocateTableIndex.getGroup(table.getId()).grpId, List.of());
        LakeRangeRewriteSchemaChangeJob job =
                createRewriteJob("t_colo_no_ranges", "alter table t_colo_no_ranges order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));

        AlterCancelException e = assertThrows(AlterCancelException.class, job::runPendingJob);

        assertTrue(e.getMessage().contains("colocate range metadata is missing"), e.getMessage());
    }

    // ---------------------------------------------------------------------------------------------
    // PACK placement of the shadow tablets before the job finishes rewriting
    // ---------------------------------------------------------------------------------------------

    /** Fails the test if a tablet is to be placed into a PACK shard group. */
    private static void forbidPackPlacement() {
        new MockUp<ColocateChecker>() {
            @Mock
            public boolean reconcileTabletPackPlacement(Map<Long, Range<Tuple>> tabletIdToRange,
                                                        List<ColocateRange> expectedRanges, int colocateColumnCount,
                                                        String logContext) {
                throw new AssertionError("no tablet may be placed into a PACK shard group: " + logContext);
            }
        };
    }

    /**
     * The shadow tablets are created in the partition's shard group only. Once every partition is rewritten, the job
     * places each of them into the PACK shard group of its ColocateRange, and stays RUNNING until a later round finds
     * them all placed. A new leader that takes the job over from the journal runs the round again and finishes.
     */
    @Test
    public void testRunningJobPlacesShadowTabletsIntoPackGroupsBeforeFinishingRewrite() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_pack", "");
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        LakeRangeRewriteSchemaChangeJob job = submitRewrite("t_colo_pack", "alter table t_colo_pack order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));
        runToRunning(job);
        MaterializedIndex shadowIndex = job.getShadowIndex(partition.getId());
        long partitionShardGroupId = partition.getLatestBaseIndex().getShardGroupId();
        assertEquals(Set.of(List.of(partitionShardGroupId)), new HashSet<>(shardGroupIdsOf(shadowIndex).values()));
        markRewritesPublished(job);

        job.runRunningJob();

        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        assertEquals("every partition is rewritten, waiting for PACK placement of 3 shadow tablets", job.errMsg);
        assertInPackGroupsOfColocateRanges(table, shadowIndex, job.getNewSortKeyColumns());
        // What a new leader replays from the journal at this point.
        LakeRangeRewriteSchemaChangeJob takenOver = (LakeRangeRewriteSchemaChangeJob)
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job.copyForPersist()), AlterJobV2.class);

        job.runRunningJob();

        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        assertEquals("", job.errMsg);

        // The new leader runs the round again: the tablets are already placed, so it finishes rewriting.
        assertEquals(AlterJobV2.JobState.RUNNING, takenOver.getJobState());
        takenOver.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, takenOver.getJobState());
        assertEquals("", takenOver.errMsg);
        assertInPackGroupsOfColocateRanges(table, shadowIndex, job.getNewSortKeyColumns());
    }

    /**
     * A split of a peer table can add a ColocateRange bound inside a shadow tablet after the shadow was planned. That
     * tablet has no single PACK shard group, so it is left to the colocate checker, which aligns the table after the
     * flip; the other shadow tablets are placed against the current ColocateRanges.
     */
    @Test
    public void testShadowTabletCrossingColocateBoundAddedAfterPlanningIsNotPlaced() throws Exception {
        holdReshardDaemon();
        OlapTable table = createDupTableInThreeRangeGroup("t_colo_pack_peer", "");
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_colo_pack_peer", "alter table t_colo_pack_peer order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));
        runToRunning(job);
        // The peer split cuts [100, 200) at 150: [100, 150) keeps its PACK shard group, [150, 200) gets a new one.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;
        List<ColocateRange> plannedRanges = colocateTableIndex.getColocateRanges(grpId);
        long newShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db().getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(grpId, List.of(plannedRanges.get(0),
                new ColocateRange(Range.gelt(intTuple(100), intTuple(150)), plannedRanges.get(1).getShardGroupId()),
                new ColocateRange(Range.gelt(intTuple(150), intTuple(200)), newShardGroupId),
                plannedRanges.get(2)));
        markRewritesPublished(job);

        job.runRunningJob();

        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        assertEquals("every partition is rewritten, waiting for PACK placement of 2 shadow tablets", job.errMsg);
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        List<ColocateRange> currentRanges = colocateRanges(table);
        ColocateRangeUtils.Classifier classifier =
                ColocateRangeUtils.Classifier.of(currentRanges, job.getNewSortKeyColumns(), 1);
        MaterializedIndex shadowIndex = job.getShadowIndex(partition.getId());
        Map<Long, List<Long>> groupIds = shardGroupIdsOf(shadowIndex);
        long partitionShardGroupId = partition.getLatestBaseIndex().getShardGroupId();
        int crossingTabletCount = 0;
        for (Tablet tablet : shadowIndex.getTablets()) {
            int rangeIndex = classifier.indexOf(tablet);
            Set<Long> expectedGroupIds = rangeIndex < 0
                    ? Set.of(partitionShardGroupId)
                    : Set.of(partitionShardGroupId, currentRanges.get(rangeIndex).getShardGroupId());
            assertEquals(expectedGroupIds, new HashSet<>(groupIds.get(tablet.getId())), tablet.getRange().toString());
            crossingTabletCount += rangeIndex < 0 ? 1 : 0;
        }
        assertEquals(1, crossingTabletCount);
    }

    /** Outside a range colocate group the job finishes rewriting in the round that finds every partition rewritten. */
    @Test
    public void testRewriteOutsideColocateGroupFinishesRewritingWithoutPackPlacement() throws Exception {
        holdReshardDaemon();
        starRocksAssert.withTable("create table t_no_colo_pack (k1 int, k2 int, k3 int, v1 int)\n"
                + "duplicate key(k1, k2, k3)\norder by(k1, k2, k3)\nproperties('replication_num' = '1');");
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_no_colo_pack", "alter table t_no_colo_pack order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));
        runToRunning(job);
        markRewritesPublished(job);
        forbidPackPlacement();

        job.runRunningJob();

        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        assertEquals("", job.errMsg);
    }

    /**
     * The colocate prefix rule is checked for the last time under the table lock before the shadow index is installed.
     * The table stays in SCHEMA_CHANGE until the flip, so the rule cannot change while the job runs, and a RUNNING job
     * that has rewritten every partition is not checked again.
     */
    @Test
    public void testRunningJobDoesNotCheckColocatePrefixAgain() throws Exception {
        holdReshardDaemon();
        createDupColocateTable("t_colo_pack_recheck", "g_colo_pack_recheck:k1");
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_colo_pack_recheck", "alter table t_colo_pack_recheck order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));
        runToRunning(job);
        new MockUp<SchemaChangeHandler>() {
            @Mock
            void checkRangeColocatePrefix(OlapTable table, List<Column> newRoutingColumns) {
                throw new AssertionError("a RUNNING job must not check the colocate prefix rule again");
            }
        };

        runRunningJobToFinished(job);
    }

    // ---------------------------------------------------------------------------------------------
    // The flip, its replay, and the colocate_with property
    // ---------------------------------------------------------------------------------------------

    /**
     * Creates the DUP table {@code name} (k1, k2, k3, v1) sorted by (k1, k2, k3) in a group with three ColocateRanges
     * on k1, and the peer table {@code <name>_peer} (k1, v2) in the same group.
     */
    private static OlapTable createThreeRangeTableWithPeer(String name) throws Exception {
        OlapTable table = createDupTableInThreeRangeGroup(name, "");
        createColocateTable(name + "_peer", "k1 int, v2 int", "duplicate key(k1)", null, "g_" + name + ":k1");
        return table;
    }

    /**
     * A sample of the sort key (k1, k3, k2), four tuples in each of the three ColocateRanges on k1: with six tablets
     * requested, it cuts them into six shadow tablets.
     */
    private static Sampler sixTabletSampler() {
        return stubSampler(List.of(
                intTuple(10, 1, 1), intTuple(20, 1, 1), intTuple(30, 1, 1), intTuple(40, 1, 1),
                intTuple(120, 5, 5), intTuple(150, 1, 1), intTuple(170, 1, 1), intTuple(180, 1, 1),
                intTuple(200, 1, 1), intTuple(250, 1, 1), intTuple(300, 1, 1), intTuple(350, 1, 1)));
    }

    /**
     * Submits the rewrite of a table made by {@link #createThreeRangeTableWithPeer} to the sort key (k1, k3, k2), with
     * a sample that cuts its three ColocateRanges into six shadow tablets.
     */
    private static LakeRangeRewriteSchemaChangeJob submitSampledRewrite(String name) throws Exception {
        LakeRangeRewriteSchemaChangeJob job = submitRewrite(name, "alter table " + name + " order by (k1, k3, k2)");
        job.setSampler(sixTabletSampler());
        mockRequestedTabletCount(6);
        return job;
    }

    /**
     * Records every alter job entry written to the edit log from now on, serialized when it is written, which is what
     * a follower reads.
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

    private static OlapTable tableIn(LocalMetastore metastore, String name) {
        return (OlapTable) metastore.getTable("test", name);
    }

    /** Replays the journal entry into the handler against the given catalog, as a follower does. */
    private static void replayOn(LocalMetastore metastore, SchemaChangeHandler handler, AlterJobV2 entry) {
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(metastore);
        try {
            handler.replayAlterJobV2(entry);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }
    }

    /**
     * The layout of a table: its state, routing key and colocate_with property, and the versions, indexes and base
     * tablets of each physical partition.
     */
    private static List<Object> layoutOf(OlapTable table) {
        List<Object> layout = new ArrayList<>();
        layout.add(table.getState());
        layout.add(table.getBaseIndexMetaId());
        layout.add(MetaUtils.getRangeDistributionColumns(table).stream().map(Column::getName).collect(Collectors.toList()));
        layout.add(table.getColocateGroup());
        List<PhysicalPartition> partitions = new ArrayList<>(table.getPhysicalPartitions());
        partitions.sort(Comparator.comparingLong(PhysicalPartition::getId));
        for (PhysicalPartition partition : partitions) {
            layout.add(partition.getId());
            layout.add(partition.getVisibleVersion());
            layout.add(partition.getNextVersion());
            layout.add(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL).stream()
                    .map(MaterializedIndex::getMetaId).sorted().collect(Collectors.toList()));
            for (Tablet tablet : partition.getLatestBaseIndex().getTablets()) {
                layout.add(tablet.getId());
                layout.add(tablet.getRange());
            }
        }
        return layout;
    }

    /** Asserts that the base index of every partition of the table maps each of its tablets to a ColocateRange. */
    private static void assertBucketSeqComputed(OlapTable table) {
        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        assertNotNull(dispatch);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            assertNotNull(dispatch.computeBucketSeq(partition.getLatestBaseIndex()),
                    "not aligned: partition " + partition.getId());
        }
    }

    /** Asserts that {@code showCreateStmt}, a SHOW CREATE TABLE or MATERIALIZED VIEW, shows colocate_with {@code expected}. */
    private static void assertShownColocateWith(String showCreateStmt, String expected) throws Exception {
        String createStmt = starRocksAssert.showCreateTable(showCreateStmt);
        assertTrue(createStmt.contains("\"colocate_with\" = \"" + expected + "\""), createStmt);
    }

    private static void createTableLike(String name, String existingName) throws Exception {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(
                "create table " + name + " like " + existingName, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt.getCreateTableStmt());
    }

    /**
     * After the flip the new index is the visible base index, aligned with the ColocateRanges and in their PACK shard
     * groups, and the group stays stable: a join with a peer of the group is planned colocated and schedules.
     */
    @Test
    public void testFinishedRewriteIsAlignedAndJoinsItsPeerColocated() throws Exception {
        holdReshardDaemon();
        OlapTable table = createThreeRangeTableWithPeer("t_colo_flip");
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        LakeRangeRewriteSchemaChangeJob job = submitSampledRewrite("t_colo_flip");

        runToRunning(job);
        runRunningJobToFinished(job);

        MaterializedIndex newBaseIndex = partition.getLatestBaseIndex();
        assertEquals(job.getShadowIndexMetaId(), newBaseIndex.getMetaId());
        assertEquals(MaterializedIndex.IndexState.NORMAL, newBaseIndex.getState());
        assertEquals(List.of(newBaseIndex), partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE));
        assertEquals(6, newBaseIndex.getTablets().size());
        assertBucketSeqComputed(table);
        assertInPackGroupsOfColocateRanges(table, newBaseIndex, MetaUtils.getRangeDistributionColumns(table));
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        assertFalse(colocateTableIndex.isGroupUnstable(colocateTableIndex.getGroup(table.getId())));
        assertJoinPlannedColocated("select t.v1, p.v2 from t_colo_flip t join t_colo_flip_peer p on t.k1 = p.k1", true);
    }

    /**
     * A follower that replays the whole journal of the job, and a frontend that recovers from an image taken at
     * WAITING_TXN and replays the rest of it, end with the layout the leader has after the flip.
     */
    @Test
    public void testFollowerReplayAndRecoveryFromWaitingTxnImageGiveTheLeaderLayout() throws Exception {
        holdReshardDaemon();
        List<String> journal = recordAlterJobJournal();
        OlapTable table = createThreeRangeTableWithPeer("t_colo_replay");
        UtFrameUtils.PseudoImage imageBeforeJob = saveMetastore();
        LakeRangeRewriteSchemaChangeJob job = submitSampledRewrite("t_colo_replay");
        job.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        UtFrameUtils.PseudoImage waitingTxnImage = saveMetastore();
        // An image holds each alter job as GSON.
        String waitingTxnJobImage = GsonUtils.GSON.toJson(job, AlterJobV2.class);
        int entriesInWaitingTxnImage = journaledEntries(journal, job.getJobId()).size();
        runWaitingTxnJobToRunning(job);
        runRunningJobToFinished(job);
        List<Object> leaderLayout = layoutOf(table);

        // Each frontend deserializes the entries it replays: a replay installs objects of the entry in its catalog.
        LocalMetastore followerMetastore = loadMetastore(imageBeforeJob);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        for (AlterJobV2 entry : journaledEntries(journal, job.getJobId())) {
            replayOn(followerMetastore, followerHandler, entry);
        }
        OlapTable followerTable = tableIn(followerMetastore, "t_colo_replay");
        assertEquals(leaderLayout, layoutOf(followerTable));
        assertBucketSeqComputed(followerTable);

        LocalMetastore recoveredMetastore = loadMetastore(waitingTxnImage);
        SchemaChangeHandler recoveredHandler = new SchemaChangeHandler();
        recoveredHandler.addAlterJobV2(GsonUtils.GSON.fromJson(waitingTxnJobImage, AlterJobV2.class));
        List<AlterJobV2> entries = journaledEntries(journal, job.getJobId());
        for (AlterJobV2 entry : entries.subList(entriesInWaitingTxnImage, entries.size())) {
            replayOn(recoveredMetastore, recoveredHandler, entry);
        }
        OlapTable recoveredTable = tableIn(recoveredMetastore, "t_colo_replay");
        assertEquals(leaderLayout, layoutOf(recoveredTable));
        assertBucketSeqComputed(recoveredTable);
    }

    /**
     * A colocate_with that names no columns means the whole routing key. A rewrite that lengthens the routing key sets
     * the explicit form, naming the columns it stood for, with the flip: every journal entry of the job carries it, a
     * follower sets it when it replays the flip, and an image taken afterwards holds it. CREATE TABLE LIKE then makes
     * a table that fits the group, and ADD PARTITION works.
     */
    @Test
    public void testRewriteLengtheningImplicitColocateColumnsPinsThemAtTheFlip() throws Exception {
        holdReshardDaemon();
        List<String> journal = recordAlterJobJournal();
        createColocateTable("t_colo_pin", "k1 int, k2 int, k3 int, v1 int",
                "duplicate key(k1, k2, k3)\npartition by range(k3) (partition p1 values less than ('100'))",
                "k1, k2", "g_colo_pin");
        OlapTable table = table("t_colo_pin");
        UtFrameUtils.PseudoImage imageBeforeJob = saveMetastore();
        LakeRangeRewriteSchemaChangeJob job = submitRewrite("t_colo_pin", "alter table t_colo_pin order by (k1, k2, k3)");
        job.setSampler(stubSampler(List.of()));
        assertEquals("g_colo_pin:k1,k2", job.getPinnedColocateWith());

        runToRunning(job);
        assertShownColocateWith("show create table t_colo_pin", "g_colo_pin");
        runRunningJobToFinished(job);

        assertEquals(List.of("k1", "k2", "k3"),
                MetaUtils.getRangeDistributionColumns(table).stream().map(Column::getName).collect(Collectors.toList()));
        assertShownColocateWith("show create table t_colo_pin", "g_colo_pin:k1,k2");
        List<AlterJobV2> entries = journaledEntries(journal, job.getJobId());
        assertEquals(AlterJobV2.JobState.FINISHED, entries.get(entries.size() - 1).getJobState());
        for (AlterJobV2 entry : entries) {
            assertEquals("g_colo_pin:k1,k2", ((LakeRangeRewriteSchemaChangeJob) entry).getPinnedColocateWith(),
                    "entry " + entry.getJobState());
        }
        // The follower keeps the job of the first entry and replays each later entry into it.
        LocalMetastore followerMetastore = loadMetastore(imageBeforeJob);
        OlapTable followerTable = tableIn(followerMetastore, "t_colo_pin");
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        for (AlterJobV2 entry : entries) {
            assertEquals("g_colo_pin", followerTable.getColocateGroup(), "before entry " + entry.getJobState());
            replayOn(followerMetastore, followerHandler, entry);
        }
        assertSame(entries.get(0), followerHandler.getAlterJobsV2().get(job.getJobId()));
        assertEquals("g_colo_pin:k1,k2", followerTable.getColocateGroup());
        assertEquals("g_colo_pin:k1,k2", tableIn(loadMetastore(saveMetastore()), "t_colo_pin").getColocateGroup());

        createTableLike("t_colo_pin_like", "t_colo_pin");
        assertEquals("g_colo_pin:k1,k2", table("t_colo_pin_like").getColocateGroup());
        starRocksAssert.alterTable("alter table t_colo_pin add partition p2 values less than ('200')");
        assertBucketSeqComputed(table);
    }

    @Test
    public void testCancelledRewriteKeepsImplicitColocateColumns() throws Exception {
        holdReshardDaemon();
        createColocateTable("t_colo_pin_cancel", "k1 int, k2 int, k3 int, v1 int", "duplicate key(k1, k2, k3)",
                "k1, k2", "g_colo_pin_cancel");
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_colo_pin_cancel", "alter table t_colo_pin_cancel order by (k1, k2, k3)");
        job.setSampler(stubSampler(List.of()));
        assertEquals("g_colo_pin_cancel:k1,k2", job.getPinnedColocateWith());
        runToRunning(job);

        assertTrue(job.cancel("cancelled by the test"));

        assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertShownColocateWith("show create table t_colo_pin_cancel", "g_colo_pin_cancel");
    }

    /** Nothing is pinned for a colocate_with that names its columns, or for a routing key that keeps its length. */
    @Test
    public void testColocateColumnsNotPinnedWhenExplicitOrRoutingKeyKeepsItsLength() throws Exception {
        createDupColocateTable("t_colo_pin_explicit", "g_colo_pin_explicit:k1");
        assertNull(createRewriteJob("t_colo_pin_explicit", "alter table t_colo_pin_explicit order by (k1, k3, k2)")
                .getPinnedColocateWith());
        createKeyDerivedColocateTable("t_colo_pin_explicit_add", "g_colo_pin_explicit_add:k1");
        AlterJobV2 addJob = createJob("t_colo_pin_explicit_add",
                "alter table t_colo_pin_explicit_add add column k3 int key default '0'");
        assertNull(((LakeTableAsyncFastSchemaChangeJob) addJob).getPinnedColocateWith());
        createDupColocateTable("t_colo_pin_same", "g_colo_pin_same");
        assertNull(createRewriteJob("t_colo_pin_same", "alter table t_colo_pin_same order by (k1, k2, k3)")
                .getPinnedColocateWith());
    }

    /**
     * The explicit form is parsed with its group name and column names trimmed and its columns split at ','. A group
     * name with a trailing space, or a colocate column whose name holds a ',', would not survive that, so a change that
     * lengthens the sort key is rejected: left implicit, the property would stand for the longer sort key, and CREATE
     * TABLE LIKE of the table would fail. The rewrite, the trailing key add and ALTER MATERIALIZED VIEW ... ORDER BY all
     * reject it before anything changes, and CREATE TABLE LIKE still works.
     */
    @Test
    public void testLengtheningSortKeyRejectedWhenImplicitColocateColumnsCannotBePinned() throws Exception {
        createColocateTable("t_colo_pin_space", "k1 int, k2 int, k3 int, v1 int", "duplicate key(k1, k2, k3)",
                "k1, k2", "g_colo_pin_space ");
        assertPinRejected("t_colo_pin_space", "alter table t_colo_pin_space order by (k1, k2, k3)");
        createTableLike("t_colo_pin_space_like", "t_colo_pin_space");
        assertEquals("g_colo_pin_space ", table("t_colo_pin_space_like").getColocateGroup());

        createColocateTable("t_colo_pin_comma", "`k,1` int, k2 int, k3 int, v1 int", "duplicate key(`k,1`, k2, k3)",
                "`k,1`, k2", "g_colo_pin_comma");
        assertPinRejected("t_colo_pin_comma", "alter table t_colo_pin_comma order by (`k,1`, k2, k3)");
        createTableLike("t_colo_pin_comma_like", "t_colo_pin_comma");
        assertEquals("g_colo_pin_comma", table("t_colo_pin_comma_like").getColocateGroup());

        createKeyDerivedColocateTable("t_colo_pin_space_add", "g_colo_pin_space_add ");
        assertPinRejected("t_colo_pin_space_add", "alter table t_colo_pin_space_add add column k3 int key default '0'");

        createMvBaseTable("t_mv_pin_space");
        createRangeMv("mv_colo_pin_space", "t_mv_pin_space", "k1, k2", "g_mv_colo_pin_space ");
        assertPinRejected("mv_colo_pin_space", "alter materialized view mv_colo_pin_space order by (k1, k2, k3)");
    }

    /**
     * Runs {@code ddl}, which lengthens the sort key of {@code tableName}, and checks it is rejected because the
     * colocate_with of the table can not be pinned, before anything changes: no alter job is registered, and the table
     * keeps its state, its colocate_with and its sort key.
     */
    private static void assertPinRejected(String tableName, String ddl) {
        OlapTable table = table(tableName);
        String colocateWith = table.getColocateGroup();
        List<Column> routingColumns = MetaUtils.getRangeDistributionColumns(table);
        Set<Long> jobIdsBefore = new HashSet<>(handler().getAlterJobsV2().keySet());

        String message = assertRejected(() -> starRocksAssert.ddl(ddl), PIN_REJECTED_MESSAGE);

        assertTrue(message.contains("The colocate_with '" + colocateWith + "' of "
                + (table.isMaterializedView() ? "materialized view " : "table ") + tableName + " names only"), message);
        assertEquals(jobIdsBefore, handler().getAlterJobsV2().keySet(), "no alter job may be registered");
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertEquals(colocateWith, table.getColocateGroup());
        assertEquals(routingColumns, MetaUtils.getRangeDistributionColumns(table));
    }

    private static LakeTableAsyncFastSchemaChangeJob submitTrailingKeyAdd(String tableName) throws Exception {
        starRocksAssert.alterTable("alter table " + tableName + " add column k3 int key default '0'");
        return (LakeTableAsyncFastSchemaChangeJob) soleUnfinishedJob(tableName);
    }

    private static void runMetadataOnlyJobToFinished(LakeTableAsyncFastSchemaChangeJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    /** The metadata-only trailing key add pins an implicit colocate_with with its catalog update, as the rewrite does. */
    @Test
    public void testTrailingKeyAddPinsImplicitColocateColumns() throws Exception {
        holdReshardDaemon();
        List<String> journal = recordAlterJobJournal();
        createColocateTable("t_colo_pin_add", "k1 int NOT NULL, k2 int NOT NULL, v1 int",
                "duplicate key(k1, k2)\npartition by range(k2) (partition p1 values less than ('100'))", null,
                "g_colo_pin_add");
        OlapTable table = table("t_colo_pin_add");
        UtFrameUtils.PseudoImage imageBeforeJob = saveMetastore();
        LakeTableAsyncFastSchemaChangeJob job = submitTrailingKeyAdd("t_colo_pin_add");
        assertEquals("g_colo_pin_add:k1,k2", job.getPinnedColocateWith());
        assertShownColocateWith("show create table t_colo_pin_add", "g_colo_pin_add");

        runMetadataOnlyJobToFinished(job);

        assertEquals(List.of("k1", "k2", "k3"),
                MetaUtils.getRangeDistributionColumns(table).stream().map(Column::getName).collect(Collectors.toList()));
        assertShownColocateWith("show create table t_colo_pin_add", "g_colo_pin_add:k1,k2");
        List<AlterJobV2> entries = journaledEntries(journal, job.getJobId());
        assertEquals(AlterJobV2.JobState.FINISHED, entries.get(entries.size() - 1).getJobState());
        for (AlterJobV2 entry : entries) {
            assertEquals("g_colo_pin_add:k1,k2", ((LakeTableAsyncFastSchemaChangeJob) entry).getPinnedColocateWith(),
                    "entry " + entry.getJobState());
        }
        LocalMetastore followerMetastore = loadMetastore(imageBeforeJob);
        OlapTable followerTable = tableIn(followerMetastore, "t_colo_pin_add");
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        for (AlterJobV2 entry : entries) {
            assertEquals("g_colo_pin_add", followerTable.getColocateGroup(), "before entry " + entry.getJobState());
            replayOn(followerMetastore, followerHandler, entry);
        }
        assertEquals("g_colo_pin_add:k1,k2", followerTable.getColocateGroup());
        // A catalog update that finds the new schema already installed returns early; it still sets the property.
        followerTable.setColocateGroup("g_colo_pin_add");
        ((LakeTableAsyncFastSchemaChangeJob) entries.get(entries.size() - 1))
                .updateCatalog(followerMetastore.getDb("test"), followerTable, true);
        assertEquals("g_colo_pin_add:k1,k2", followerTable.getColocateGroup());
        assertEquals("g_colo_pin_add:k1,k2", tableIn(loadMetastore(saveMetastore()), "t_colo_pin_add").getColocateGroup());

        createTableLike("t_colo_pin_add_like", "t_colo_pin_add");
        assertEquals("g_colo_pin_add:k1,k2", table("t_colo_pin_add_like").getColocateGroup());
        starRocksAssert.alterTable("alter table t_colo_pin_add add partition p2 values less than ('200')");
        assertBucketSeqComputed(table);
    }

    @Test
    public void testCancelledTrailingKeyAddKeepsImplicitColocateColumns() throws Exception {
        holdReshardDaemon();
        createKeyDerivedColocateTable("t_colo_pin_add_cancel", "g_colo_pin_add_cancel");
        LakeTableAsyncFastSchemaChangeJob job = submitTrailingKeyAdd("t_colo_pin_add_cancel");
        assertEquals("g_colo_pin_add_cancel:k1,k2", job.getPinnedColocateWith());
        job.runPendingJob();

        assertTrue(job.cancel("cancelled by the test"));

        assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertShownColocateWith("show create table t_colo_pin_add_cancel", "g_colo_pin_add_cancel");
    }

    /** A catalog update of the trailing key add that fails its checks leaves an implicit colocate_with as it is. */
    @Test
    public void testTrailingKeyAddCatalogUpdateFailingItsChecksKeepsImplicitColocateColumns() throws Exception {
        holdReshardDaemon();
        createKeyDerivedColocateTable("t_colo_pin_add_check", "g_colo_pin_add_check");
        OlapTable table = table("t_colo_pin_add_check");
        LakeTableAsyncFastSchemaChangeJob job = submitTrailingKeyAdd("t_colo_pin_add_check");
        assertEquals("g_colo_pin_add_check:k1,k2", job.getPinnedColocateWith());
        // The table is at a schema version newer than the one the job installs.
        MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        indexMeta.setSchemaVersion(indexMeta.getSchemaVersion() + 2);

        assertThrows(IllegalStateException.class, () -> job.updateCatalog(db(), table, true));

        assertEquals("g_colo_pin_add_check", table.getColocateGroup());
    }

    // ---------------------------------------------------------------------------------------------
    // A split of a peer table while the rewrite runs
    // ---------------------------------------------------------------------------------------------

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

    /** Plans the join, checks whether it is planned colocated, and starts scheduling it. */
    private static void assertJoinPlannedColocated(String join, boolean colocated) throws Exception {
        String plan = UtFrameUtils.getPlanAndFragment(connectContext, join).second.getExplainString(TExplainLevel.NORMAL);
        assertEquals(colocated, plan.contains("INNER JOIN (COLOCATE)"), plan);
        UtFrameUtils.getPlanAndStartScheduling(connectContext, join);
    }

    /**
     * A split of a peer table that lands after the shadow index was planned adds a ColocateRange bound inside a shadow
     * tablet and marks the group unstable. While the table is in SCHEMA_CHANGE the colocate checker leaves it alone, so
     * the group stays unstable and a join with the peer is not planned colocated. The job still finishes: the shadow
     * tablet that crosses the new bound is not placed into a PACK shard group. After the flip the checker submits one
     * alignment split of the new index, and once that split is published the next cycle marks the group stable.
     */
    @Test
    public void testPeerSplitAfterPlanningIsAlignedByColocateCheckerAfterFlip() throws Exception {
        holdReshardDaemon();
        Map<Long, List<TabletRange>> chosenSplits = new HashMap<>();
        answerSplitPublishes(chosenSplits);
        OlapTable table = createThreeRangeTableWithPeer("t_colo_peer_split");
        OlapTable peer = table("t_colo_peer_split_peer");
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(table.getId());
        String join = "select t.v1, p.v2 from t_colo_peer_split t join t_colo_peer_split_peer p on t.k1 = p.k1";
        LakeRangeRewriteSchemaChangeJob job =
                submitRewrite("t_colo_peer_split", "alter table t_colo_peer_split order by (k1, k3, k2)");
        job.setSampler(stubSampler(List.of()));
        job.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        assertOneTabletPerColocateRange(table, job.getShadowIndex(partition.getId()), job.getNewSortKeyColumns());
        assertFalse(colocateTableIndex.isGroupUnstable(groupId));

        splitPeerTabletAt150(peer, chosenSplits);

        assertEquals(4, colocateRanges(table).size());
        assertTrue(colocateTableIndex.isGroupUnstable(groupId));
        assertBucketSeqComputed(peer);
        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();
        assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        assertTrue(colocateTableIndex.isGroupUnstable(groupId));
        assertEquals(List.of(), splitJobsOf(table));
        assertEquals(1, splitJobsOf(peer).size(), "the split peer is aligned and needs no alignment split");
        assertJoinPlannedColocated(join, false);

        runWaitingTxnJobToRunning(job);
        markRewritesPublished(job);
        job.runRunningJob();
        // The shadow tablet of [100, 200) crosses the new bound 150, so only the other two are placed.
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        assertEquals("every partition is rewritten, waiting for PACK placement of 2 shadow tablets", job.errMsg);
        job.runRunningJob();
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        job.setPublishExecutor(publishingJob -> true);
        job.runFinishedRewritingJob();
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        MaterializedIndex newBaseIndex = partition.getLatestBaseIndex();
        assertEquals(job.getShadowIndexMetaId(), newBaseIndex.getMetaId());
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertNull(RangeColocateScanDispatch.forTable(table).computeBucketSeq(newBaseIndex));
        assertTrue(colocateTableIndex.isGroupUnstable(groupId));
        assertJoinPlannedColocated(join, false);

        checker.runOneCycle();

        List<TabletReshardJob> alignmentSplits = splitJobsOf(table);
        assertEquals(1, alignmentSplits.size());
        assertEquals(List.of(newBaseIndex.getId()), alignmentSplits.get(0).getReshardingPhysicalPartitions()
                .get(partition.getId()).getReshardingIndexes().values().stream()
                .map(reshardingIndex -> reshardingIndex.getMaterializedIndexId()).collect(Collectors.toList()));
        assertEquals(1, splitJobsOf(peer).size());
        assertTrue(colocateTableIndex.isGroupUnstable(groupId));

        runReshardJobToFinished(alignmentSplits.get(0));
        checker.runOneCycle();

        MaterializedIndex alignedIndex = partition.getLatestBaseIndex();
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        ColocateRangeUtils.Classifier classifier =
                ColocateRangeUtils.Classifier.of(colocateRanges(table), sortKeyColumns, 1);
        assertEquals(List.of(0, 1, 2, 3), alignedIndex.getTablets().stream()
                .map(tablet -> classifier.indexOf(tablet)).sorted().collect(Collectors.toList()));
        assertInPackGroupsOfColocateRanges(table, alignedIndex, sortKeyColumns);
        assertFalse(colocateTableIndex.isGroupUnstable(groupId));
        assertJoinPlannedColocated(join, true);
    }

    // ---------------------------------------------------------------------------------------------
    // Metadata-only trailing key add on a table in a range colocate group
    // ---------------------------------------------------------------------------------------------

    /**
     * Adding a trailing key column appends a NULL to each bound of every tablet, which keeps each tablet inside its
     * ColocateRange: the tablets stay aligned, the group stays stable, a join with a peer is planned colocated and
     * schedules, and a table created afterwards still joins the group.
     */
    @Test
    public void testTrailingKeyAddKeepsColocateTabletsAligned() throws Exception {
        holdReshardDaemon();
        OlapTable table = createInThreeRangeGroup("t_colo_tail", "k1 int, k2 int, v1 int", "duplicate key(k1, k2)", null);
        createColocateTable("t_colo_tail_peer", "k1 int, v2 int", "duplicate key(k1)", null, "g_t_colo_tail:k1");
        LakeTableAsyncFastSchemaChangeJob job = submitTrailingKeyAdd("t_colo_tail");

        runMetadataOnlyJobToFinished(job);

        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        assertEquals(List.of("k1", "k2", "k3"), sortKeyColumns.stream().map(Column::getName).collect(Collectors.toList()));
        MaterializedIndex baseIndex = table.getPhysicalPartitions().iterator().next().getLatestBaseIndex();
        assertEquals(List.of(intTuple(100, null, null), intTuple(200, null, null)), baseIndex.getTablets().stream()
                .map(tablet -> tablet.getRange().getRange()).filter(range -> !range.isMaximum())
                .map(Range::getUpperBound).collect(Collectors.toList()));
        assertOneTabletPerColocateRange(table, baseIndex, sortKeyColumns);
        assertAligned(table, baseIndex, sortKeyColumns);
        assertBucketSeqComputed(table);
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        assertFalse(colocateTableIndex.isGroupUnstable(colocateTableIndex.getGroup(table.getId())));
        assertJoinPlannedColocated("select t.v1, p.v2 from t_colo_tail t join t_colo_tail_peer p on t.k1 = p.k1", true);

        createColocateTable("t_colo_tail_new", "k1 int, k2 int, k3 int, v1 int", "duplicate key(k1, k2, k3)", null,
                "g_t_colo_tail:k1");
        OlapTable newMember = table("t_colo_tail_new");
        assertTrue(colocateTableIndex.isSameGroup(table.getId(), newMember.getId()));
        assertBucketSeqComputed(newMember);
        assertJoinPlannedColocated("select t.v1, n.v1 from t_colo_tail t join t_colo_tail_new n on t.k1 = n.k1", true);
    }

    // ---------------------------------------------------------------------------------------------
    // ALTER MATERIALIZED VIEW ... ORDER BY on a materialized view in a range colocate group
    // ---------------------------------------------------------------------------------------------

    /** A range DUP table (k1, k2, k3, v1) outside any colocate group, for materialized views to select from. */
    private static void createMvBaseTable(String name) throws Exception {
        starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, k3 int, v1 int)\n"
                + "duplicate key(k1, k2, k3)\n"
                + "properties('replication_num' = '1');");
    }

    /**
     * Creates the range distributed async materialized view {@code name} selecting (k1, k2, k3, v1) from {@code baseTable},
     * sorted by {@code orderBy}, in the colocate group {@code colocateWith}, or in none when it is null.
     */
    private static MaterializedView createRangeMv(String name, String baseTable, String orderBy, String colocateWith)
            throws Exception {
        boolean savedEnableRangeDistribution = connectContext.getSessionVariable().isEnableRangeDistribution();
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        try {
            starRocksAssert.withMaterializedView("create materialized view " + name + "\n"
                    + "order by (" + orderBy + ")\n"
                    + "refresh deferred manual\n"
                    + "properties('replication_num' = '1'"
                    + (colocateWith == null ? "" : ", 'colocate_with' = '" + colocateWith + "'") + ")\n"
                    + "as select k1, k2, k3, v1 from " + baseTable);
        } finally {
            connectContext.getSessionVariable().setEnableRangeDistribution(savedEnableRangeDistribution);
        }
        MaterializedView mv = (MaterializedView) table(name);
        assertTrue(mv.isRangeDistribution());
        return mv;
    }

    /** Runs ALTER MATERIALIZED VIEW ... ORDER BY and returns the rewrite job it registered; no background handler runs it. */
    private static LakeMvSortKeyRewriteJob submitMvRewrite(String mvName, String orderBy) throws Exception {
        starRocksAssert.ddl("alter materialized view " + mvName + " order by (" + orderBy + ")");
        return (LakeMvSortKeyRewriteJob) soleUnfinishedJob(mvName);
    }

    /**
     * An ORDER BY that keeps the colocate columns in front runs to FINISHED as the rewrite of a table does: the new index
     * is aligned with the ColocateRanges and in their PACK shard groups, and a join with a table of the group is planned
     * colocated and schedules.
     */
    @Test
    public void testMvOrderByKeepingColocatePrefixIsAlignedAndJoinsItsGroupColocated() throws Exception {
        holdReshardDaemon();
        createDupTableInThreeRangeGroup("t_colo_mv", "");
        MaterializedView mv = createRangeMv("mv_colo", "t_colo_mv", "k1, k2, k3", "g_t_colo_mv:k1");
        PhysicalPartition partition = mv.getPhysicalPartitions().iterator().next();
        LakeMvSortKeyRewriteJob job = submitMvRewrite("mv_colo", "k1, k3, k2");
        job.setSampler(sixTabletSampler());
        mockRequestedTabletCount(6);

        runToRunning(job);
        runRunningJobToFinished(job);

        MaterializedIndex newBaseIndex = partition.getLatestBaseIndex();
        assertEquals(job.getShadowIndexMetaId(), newBaseIndex.getMetaId());
        assertEquals(List.of("k1", "k3", "k2"),
                MetaUtils.getRangeDistributionColumns(mv).stream().map(Column::getName).collect(Collectors.toList()));
        assertEquals(6, newBaseIndex.getTablets().size());
        assertBucketSeqComputed(mv);
        assertInPackGroupsOfColocateRanges(mv, newBaseIndex, MetaUtils.getRangeDistributionColumns(mv));
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        assertFalse(colocateTableIndex.isGroupUnstable(colocateTableIndex.getGroup(mv.getId())));
        assertJoinPlannedColocated("select m.v1, t.v1 from mv_colo m join t_colo_mv t on m.k1 = t.k1", true);
    }

    @Test
    public void testMvOrderByDroppingOrReorderingColocatePrefixRejectedAtAnalysis() throws Exception {
        createMvBaseTable("t_mv_prefix");
        createRangeMv("mv_colo_prefix", "t_mv_prefix", "k1, k2, k3", "g_mv_colo_prefix:k1,k2");
        String message = assertRejected(() -> UtFrameUtils.parseStmtWithNewParser(
                "alter materialized view mv_colo_prefix order by (k1, k3)", connectContext), MV_PREFIX_RULE_MESSAGE);
        assertTrue(message.endsWith("Detail message: The sort key of a materialized view in a range colocate group must "
                + "start with its colocate columns [k1, k2] (same order and types). Got: [k1, k3]."), message);
        assertRejected(() -> UtFrameUtils.parseStmtWithNewParser(
                "alter materialized view mv_colo_prefix order by (k2, k1, k3)", connectContext), MV_PREFIX_RULE_MESSAGE);
    }

    /**
     * The statement is analyzed while its name belongs to a materialized view outside any colocate group. SWAP WITH then
     * gives the name to a member of a range colocate group, so the colocate prefix rule rejects the statement when it
     * is checked again under the lock, before any job is registered.
     */
    @Test
    public void testMvOrderByBreakingColocatePrefixRejectedUnderLockAfterSwap() throws Exception {
        createMvBaseTable("t_mv_swap");
        createRangeMv("mv_swap", "t_mv_swap", "k1, k2, k3", null);
        MaterializedView colocateMv = createRangeMv("mv_swap_colo", "t_mv_swap", "k1, k2, k3", "g_mv_swap_colo:k1");
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter materialized view mv_swap order by (k2, k1, k3)", connectContext);
        starRocksAssert.ddl("alter materialized view mv_swap swap with mv_swap_colo");
        assertSame(colocateMv, table("mv_swap"));
        Set<Long> jobIdsBefore = new HashSet<>(handler().getAlterJobsV2().keySet());

        String message = assertRejected(
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(stmt),
                MV_PREFIX_RULE_MESSAGE);

        assertTrue(message.endsWith("Got: [k2, k1, k3]."), message);
        assertEquals(jobIdsBefore, handler().getAlterJobsV2().keySet(), "no alter job may be registered");
        assertEquals(OlapTable.OlapTableState.NORMAL, colocateMv.getState());
    }

    /**
     * A colocate_with of a materialized view that names no columns is pinned at the flip as a table's is: an ORDER BY
     * that keeps the colocate columns in front and lengthens the routing key makes SHOW CREATE MATERIALIZED VIEW show
     * the explicit form.
     */
    @Test
    public void testMvOrderByLengtheningImplicitColocateColumnsPinsThemAtTheFlip() throws Exception {
        holdReshardDaemon();
        createMvBaseTable("t_mv_pin");
        createRangeMv("mv_colo_pin", "t_mv_pin", "k1, k2", "g_mv_colo_pin");
        LakeMvSortKeyRewriteJob job = submitMvRewrite("mv_colo_pin", "k1, k2, k3");
        job.setSampler(stubSampler(List.of()));
        assertEquals("g_mv_colo_pin:k1,k2", job.getPinnedColocateWith());

        runToRunning(job);
        assertShownColocateWith("show create materialized view mv_colo_pin", "g_mv_colo_pin");
        runRunningJobToFinished(job);

        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        assertShownColocateWith("show create materialized view mv_colo_pin", "g_mv_colo_pin:k1,k2");
    }
}
