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

import com.starrocks.alter.LakeOnlineRewriteJobBase.PendingPartitionPlan;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.alter.reshard.presplit.Sampler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeRangeRollupJobTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_range_rollup_test";
    private static Database db;
    private OlapTable table;

    /** The base (origin) index meta id derived from the test table. */
    private long baseIndexMetaId;
    private long dbId;
    private long tableId;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
    }

    @BeforeEach
    public void before() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(DB_NAME);
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        String sql = "create table t_rollup (k1 int, k2 int, v1 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_rollup");
        baseIndexMetaId = table.getBaseIndexMetaId();
        dbId = db.getId();
        tableId = table.getId();
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    // ---- helpers ------------------------------------------------------------

    /** Build a minimal job ready for construction assertions. */
    private LakeRangeRollupJob newMinimalJob() {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowIndexMetaId = GlobalStateMgr.getCurrentState().getNextId();
        LakeRangeRollupJob job = new LakeRangeRollupJob(jobId, dbId, tableId, "t_rollup", 3600_000L);
        job.setShadowIndex(shadowIndexMetaId, baseIndexMetaId, "r2", (short) 1);
        return job;
    }

    /** Build a fully-configured job (all persisted fields set). */
    private LakeRangeRollupJob newFullJob() {
        LakeRangeRollupJob job = newMinimalJob();
        List<Column> rollupSchema = new ArrayList<>(table.getSchemaByIndexMetaId(baseIndexMetaId).subList(0, 2));
        job.setRollupSchema(rollupSchema);
        job.setRollupKeysType(KeysType.DUP_KEYS);
        job.setRollupSortKeyColumns(List.of(rollupSchema.get(0)));
        job.setRollupSortKeyIdxes(List.of(0));
        job.setRollupSortKeyUniqueIds(List.of(0));
        return job;
    }

    /**
     * Build an OlapTable whose physical partition holds the rollup index (metaId {@code rollupMetaId})
     * in the given state, so flipNotYetApplied can be driven directly.
     *
     * Uses {@code createRollupIndex} which registers a brand-new meta id (as a real rollup registration
     * would); the index state (SHADOW or NORMAL) is reflected correctly.
     */
    private OlapTable newTableWithShadowRollupIndex(long rollupMetaId, MaterializedIndex.IndexState state) {
        long indexPhysId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedIndex idx = new MaterializedIndex(indexPhysId, rollupMetaId, state,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        // createRollupIndex registers a brand-new meta id and routes to visible or shadow map by state.
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            pp.createRollupIndex(idx);
        }
        return table;
    }

    // ---- Step 1 / Step 4: construction test ---------------------------------

    @Test
    public void testConstructionAndJobTableState() {
        LakeRangeRollupJob job = new LakeRangeRollupJob(10L, dbId, tableId, "t", 3600_000L);
        job.setShadowIndex(2000L, baseIndexMetaId, "r2", (short) 1);
        assertEquals(AlterJobV2.JobType.ROLLUP, job.getType());
        assertEquals(OlapTable.OlapTableState.ROLLUP, job.jobTableState());
        assertEquals(2000L, job.getShadowIndexMetaId());
    }

    // ---- Step 5 / Step 6: flipNotYetApplied tests ---------------------------

    @Test
    public void testFlipNotYetAppliedTrueWhenShadow() {
        OlapTable t = newTableWithShadowRollupIndex(2000L, MaterializedIndex.IndexState.SHADOW);
        LakeRangeRollupJob job = new LakeRangeRollupJob(10L, dbId, tableId, "t", 3600_000L);
        job.setShadowIndex(2000L, baseIndexMetaId, "r2", (short) 1);
        assertTrue(job.flipNotYetApplied(t));
    }

    @Test
    public void testFlipNotYetAppliedFalseWhenNormal() {
        OlapTable t = newTableWithShadowRollupIndex(2000L, MaterializedIndex.IndexState.NORMAL);
        LakeRangeRollupJob job = new LakeRangeRollupJob(10L, dbId, tableId, "t", 3600_000L);
        job.setShadowIndex(2000L, baseIndexMetaId, "r2", (short) 1);
        assertFalse(job.flipNotYetApplied(t));
    }

    @Test
    public void testFlipNotYetAppliedTrueWhenShadowAbsent() {
        // No rollup index added to any partition -> absent -> not yet applied (treat as not-flipped so
        // visualiseShadowIndex re-runs and fails loudly via Preconditions.checkNotNull instead of silently
        // dropping the rollup).
        LakeRangeRollupJob job = new LakeRangeRollupJob(10L, dbId, tableId, "t", 3600_000L);
        job.setShadowIndex(9999L, baseIndexMetaId, "r_absent", (short) 1);
        assertTrue(job.flipNotYetApplied(table));
    }

    // ---- getInfo size test --------------------------------------------------

    @Test
    public void testGetInfoSizeMatchesRollupProcDirTitleNames() {
        LakeRangeRollupJob job = newMinimalJob();
        List<List<Comparable>> infos = new ArrayList<>();
        // getInfo is protected, but this test is in the same package so it is callable directly.
        job.getInfo(infos);
        assertEquals(1, infos.size());
        assertEquals(RollupProcDir.TITLE_NAMES.size(), infos.get(0).size(),
                "getInfo row must have exactly " + RollupProcDir.TITLE_NAMES.size() + " columns");
    }

    // ---- copyForPersist / replay tests --------------------------------------

    @Test
    public void testCopyForPersistIsDeepCopy() {
        LakeRangeRollupJob job = newFullJob();
        AlterJobV2 copy = job.copyForPersist();
        assertNotSame(job, copy);
        assertInstanceOf(LakeRangeRollupJob.class, copy);
        assertEquals(job.getShadowIndexMetaId(), ((LakeRangeRollupJob) copy).getShadowIndexMetaId());
    }

    @Test
    public void testGsonRoundTripPreservesSubtypeAndFields() {
        LakeRangeRollupJob job = newFullJob();
        AlterJobV2 copy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(copy);
        assertTrue(text.contains("\"clazz\""), "serialized text must carry the clazz discriminator");
        assertTrue(text.contains("LakeRangeRollupJob"),
                "serialized text must carry the LakeRangeRollupJob label");
        AlterJobV2 restored = GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        assertInstanceOf(LakeRangeRollupJob.class, restored);
        LakeRangeRollupJob restoredJob = (LakeRangeRollupJob) restored;
        assertEquals(job.getJobId(), restoredJob.getJobId());
        assertEquals(job.getShadowIndexMetaId(), restoredJob.getShadowIndexMetaId());
    }

    @Test
    public void testReplayTransfersFields() {
        LakeRangeRollupJob source = newFullJob();
        String text = GsonUtils.GSON.toJson(source.copyForPersist());
        LakeRangeRollupJob replayed = (LakeRangeRollupJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRollupJob target = (LakeRangeRollupJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        target.replay(replayed);
        assertEquals(source.getShadowIndexMetaId(), target.getShadowIndexMetaId());
        assertEquals(AlterJobV2.JobType.ROLLUP, target.getType());
    }

    // ---- getRollupIndexName test --------------------------------------------

    @Test
    public void testGetRollupIndexName() {
        LakeRangeRollupJob job = new LakeRangeRollupJob(1L, dbId, tableId, "t", 3600_000L);
        job.setShadowIndex(500L, baseIndexMetaId, "my_rollup", (short) 1);
        assertEquals("my_rollup", job.getRollupIndexName());
    }

    // ---- planPartitionShadow + registerShadowIndexMeta helpers ------------------

    /**
     * Build a job configured with the given rollup sort-key column list (a reordering of base columns).
     * Uses schema columns k1(idx=0) and k2(idx=1) from the base index. The supplied rollupSortKeyColumns
     * list determines which column is the primary sort key; rollupSortKeyIdxes are positional indexes into
     * the rollup schema.
     */
    private LakeRangeRollupJob newConfiguredRollupJob(List<Column> rollupSortKeyColumns) {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowIndexMetaId = GlobalStateMgr.getCurrentState().getNextId();
        LakeRangeRollupJob job = new LakeRangeRollupJob(jobId, dbId, tableId, table.getName(), 3600_000L);
        List<Column> baseSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        // Rollup schema = k1, k2 (first two columns of base).
        List<Column> rollupSchema = new ArrayList<>(baseSchema.subList(0, 2));
        job.setRollupSchema(rollupSchema);
        job.setRollupKeysType(KeysType.DUP_KEYS);
        job.setRollupSortKeyColumns(rollupSortKeyColumns);
        // Sort key idxes: position of each sort-key column in rollupSchema.
        List<Integer> sortKeyIdxes = rollupSortKeyColumns.stream()
                .map(c -> {
                    for (int i = 0; i < rollupSchema.size(); i++) {
                        if (rollupSchema.get(i).getName().equals(c.getName())) {
                            return i;
                        }
                    }
                    throw new IllegalArgumentException("sort-key column not in rollup schema: " + c.getName());
                })
                .collect(java.util.stream.Collectors.toList());
        job.setRollupSortKeyIdxes(sortKeyIdxes);
        job.setRollupSortKeyUniqueIds(sortKeyIdxes); // use same values as unique ids for tests
        job.setShadowIndex(shadowIndexMetaId, baseIndexMetaId, "r_rollup", (short) 2);
        return job;
    }

    /** A stub sampler returning a diverse sample over the given columns; drives boundary planning. */
    private static Sampler stubSamplerReturning(List<Tuple> tuples) {
        return request -> tuples.isEmpty()
                ? SampleSet.EMPTY
                : new SampleSet(tuples, new Estimates(1024L, tuples.size()));
    }

    /** Build a diverse sorted sample of (c0, c1) tuples over two integer columns. */
    private static List<Tuple> sampleOver(Column c0, Column c1) {
        List<Tuple> sample = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sample.add(new Tuple(List.of(
                    Variant.of(IntegerType.INT, Integer.toString(i)),
                    Variant.of(IntegerType.INT, Integer.toString(i)))));
        }
        return sample;
    }

    /** Create a PendingPartitionPlan for the first physical partition of the test table. */
    private PendingPartitionPlan newPendingPlan(PhysicalPartition physicalPartition) {
        MaterializedIndex baseIndex = physicalPartition.getLatestIndex(baseIndexMetaId);
        return new PendingPartitionPlan(
                physicalPartition.getId(),
                baseIndex,
                baseIndex.getShardGroupId(),
                table.getPartition(physicalPartition.getParentId()).getName(),
                baseIndex.getDataSize(),
                table.getName(),
                physicalPartition);
    }

    // ---- planPartitionShadow test (Task 3 Step 1 / Step 2 / Step 4) -------------

    @Test
    public void testPlanPartitionShadowBuildsKTabletShadowByRollupSortKey() throws Exception {
        List<Column> baseSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        // Sort key for the rollup is (k2, k1) — reordered relative to the base.
        Column colK1 = baseSchema.get(0);
        Column colK2 = baseSchema.get(1);
        LakeRangeRollupJob job = newConfiguredRollupJob(List.of(colK2, colK1));
        job.setSampler(stubSamplerReturning(sampleOver(colK2, colK1)));
        PhysicalPartition physicalPartition0 = table.getPhysicalPartitions().iterator().next();
        PendingPartitionPlan plan = newPendingPlan(physicalPartition0);
        job.planPartitionShadow(plan, table, DB_NAME);
        assertNotNull(plan.shadowIndex);
        assertEquals(plan.shadowTabletCount, plan.shadowIndex.getTablets().size());
        assertTrue(plan.shadowTabletCount >= 1);
        for (Tablet t : plan.shadowIndex.getTablets()) {
            assertNotNull(t.getRange());
        }
    }

    /**
     * The rollup shadow tablets must be created with a CreateReplicaTask carrying the ROLLUP's OWN tablet
     * schema (its single-column sort key), NOT the base index's schema (whose sort key is (k1, k2), arity
     * 2). Without this the compute node lazily materializes the rollup tablet's version-1 metadata from
     * the base index's shared initial-metadata template and reads the base sort-key arity, which breaks the
     * reshard/pre-split boundary validation.
     */
    @Test
    public void testCreateShadowTabletMetadataEmitsRollupSchema() throws Exception {
        // Resolve every rollup shadow tablet to a single alive compute node.
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                ComputeNode cn = new ComputeNode(1000L, "127.0.0.1", 9050);
                cn.setAlive(true);
                return cn;
            }
        };

        // Configure a non-default compression level so the create task must carry it through, not level 0.
        table.setCompressionLevel(7);

        List<Column> baseSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        Column colK1 = baseSchema.get(0);
        // Rollup sort key is the single column k1 (arity 1), narrower than the base's (k1, k2) (arity 2).
        LakeRangeRollupJob job = newConfiguredRollupJob(List.of(colK1));
        // Empty sample -> a single full-range shadow tablet (K=1); enough to assert the emitted schema.
        job.setSampler(stubSamplerReturning(List.of()));

        PhysicalPartition physicalPartition = table.getPhysicalPartitions().iterator().next();
        PendingPartitionPlan plan = newPendingPlan(physicalPartition);
        job.planPartitionShadow(plan, table, DB_NAME);
        assertNotNull(plan.shadowIndex);
        assertTrue(plan.shadowIndex.getTablets().size() >= 1);

        MarkedCountDownLatch<Long, Long> latch =
                new MarkedCountDownLatch<>(plan.shadowIndex.getTablets().size());
        AgentBatchTask batchTask = job.buildShadowTabletCreateTasks(table, List.of(plan), latch);

        List<AgentTask> tasks = batchTask.getAllTasks();
        assertEquals(plan.shadowIndex.getTablets().size(), tasks.size(),
                "one CreateReplicaTask per rollup shadow tablet");
        List<Integer> baseSortKeyIdxes = table.getIndexMetaByMetaId(baseIndexMetaId).getSortKeyIdxes();
        assertEquals(2, baseSortKeyIdxes.size(), "base index sort key must be (k1, k2), arity 2");

        for (AgentTask agentTask : tasks) {
            assertInstanceOf(CreateReplicaTask.class, agentTask);
            TCreateTabletReq req = ((CreateReplicaTask) agentTask).toThrift();
            // The emitted tablet schema is the rollup's own: single-column sort key [0], NOT the base's.
            assertEquals(List.of(0), req.getTablet_schema().getSort_key_idxes(),
                    "rollup shadow tablet must carry the rollup's own sort-key indexes, not the base's");
            assertEquals(job.getShadowIndexMetaId(), req.getTablet_schema().getId(),
                    "tablet schema id must be the rollup shadow index meta id");
            // Per-tablet version-1 metadata (optimization off) so the base's shared template is not used.
            assertFalse(req.isEnable_tablet_creation_optimization(),
                    "tablet-creation optimization must be off so per-tablet metadata is written");
            // The compute node overwrites the schema's compression level with this request field, so it must
            // carry the table's configured level (7), not the builder default (0).
            assertEquals(7, req.getCompression_level(),
                    "create task must carry the table's configured compression level, not the default");
        }
    }

    @Test
    public void testCreateShadowTabletMetadataSkipsWhenNoShadowTablets() {
        Column colK1 = table.getSchemaByIndexMetaId(baseIndexMetaId).get(0);
        LakeRangeRollupJob job = newConfiguredRollupJob(List.of(colK1));
        PhysicalPartition physicalPartition = table.getPhysicalPartitions().iterator().next();
        // A plan whose shadow index was never built contributes no tablets, so the method returns early
        // without acquiring the database lock or sending any create task.
        PendingPartitionPlan plan = newPendingPlan(physicalPartition);
        assertNull(plan.shadowIndex);
        assertDoesNotThrow(() -> job.createShadowTabletMetadata(List.of(plan)));
    }

    @Test
    public void testBuildShadowTabletCreateTasksThrowsWithoutComputeNode() throws Exception {
        // No alive compute node for the shadow tablet -> resolution throws, and the job aborts the alter.
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, tabletId);
            }
        };

        Column colK1 = table.getSchemaByIndexMetaId(baseIndexMetaId).get(0);
        LakeRangeRollupJob job = newConfiguredRollupJob(List.of(colK1));
        // Empty sample -> a single full-range shadow tablet; planning does not resolve per-tablet nodes.
        job.setSampler(stubSamplerReturning(List.of()));

        PhysicalPartition physicalPartition = table.getPhysicalPartitions().iterator().next();
        PendingPartitionPlan plan = newPendingPlan(physicalPartition);
        job.planPartitionShadow(plan, table, DB_NAME);
        assertNotNull(plan.shadowIndex);

        MarkedCountDownLatch<Long, Long> latch =
                new MarkedCountDownLatch<>(plan.shadowIndex.getTablets().size());
        AlterCancelException ex = assertThrows(AlterCancelException.class,
                () -> job.buildShadowTabletCreateTasks(table, List.of(plan), latch));
        assertTrue(ex.getMessage().contains("no alive compute node"),
                "abort message must name the missing compute node, was: " + ex.getMessage());
    }

    /**
     * Build a fully-configured rollup job, register its shadow index meta on the table, add the rollup
     * shadow MaterializedIndex to each physical partition (via {@code createRollupIndex}), and seed a
     * {@code commitVersion = visibleVersion + 1} in {@code partitionStates} so
     * {@code visualiseShadowIndex} can be called directly without driving the full state machine.
     */
    private LakeRangeRollupJob newConfiguredRollupJobWithReservedCommit(List<Column> rollupSortKeyColumns) {
        LakeRangeRollupJob job = newConfiguredRollupJob(rollupSortKeyColumns);
        job.registerShadowIndexMeta(table);

        long shadowMetaId = job.getShadowIndexMetaId();
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            long physId = GlobalStateMgr.getCurrentState().getNextId();
            MaterializedIndex shadowIdx = new MaterializedIndex(physId, shadowMetaId,
                    MaterializedIndex.IndexState.SHADOW, PhysicalPartition.INVALID_SHARD_GROUP_ID);
            pp.createRollupIndex(shadowIdx);
            // Reserve commitVersion = visibleVersion + 1 so visualiseShadowIndex's precondition holds.
            job.stateOf(pp.getId()).commitVersion = pp.getVisibleVersion() + 1;
        }
        return job;
    }

    // ---- registerShadowIndexMeta test (Task 3 Step 5 / Step 6 / Step 7 / Step 8) ---

    @Test
    public void testRegisterShadowIndexMetaRegistersRollupMetaWithoutTouchingBase() {
        List<Column> baseSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        Column colK1 = baseSchema.get(0);
        Column colK2 = baseSchema.get(1);
        LakeRangeRollupJob job = newConfiguredRollupJob(List.of(colK2, colK1));
        MaterializedIndexMeta baseBefore = table.getIndexMetaByMetaId(baseIndexMetaId);
        job.registerShadowIndexMeta(table);
        MaterializedIndexMeta rollupMeta = table.getIndexMetaByMetaId(job.getShadowIndexMetaId());
        assertNotNull(rollupMeta);
        assertEquals(KeysType.DUP_KEYS, rollupMeta.getKeysType());
        // Sort key idxes: colK2 is at position 1 in rollup schema (k1,k2), colK1 is at 0.
        assertEquals(List.of(1, 0), rollupMeta.getSortKeyIdxes());
        // base index meta must be untouched (identity check).
        assertSame(baseBefore, table.getIndexMetaByMetaId(baseIndexMetaId));
    }

    // ---- visualiseShadowIndex tests (Task 4) ----------------------------------------

    @Test
    public void testVisualiseShadowIndexAddsRollupAndKeepsBase() {
        List<Column> baseSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        Column colK1 = baseSchema.get(0);
        Column colK2 = baseSchema.get(1);
        LakeRangeRollupJob job = newConfiguredRollupJobWithReservedCommit(List.of(colK2, colK1));
        long baseMetaBefore = table.getBaseIndexMetaId();

        job.visualiseShadowIndex(table);

        // Rollup is now visible and NORMAL.
        assertTrue(table.getVisibleIndexMetas().stream()
                        .anyMatch(m -> m.getIndexMetaId() == job.getShadowIndexMetaId()),
                "rollup index must appear in getVisibleIndexMetas after flip");
        // Base index meta untouched.
        assertNotNull(table.getIndexMetaByMetaId(baseIndexMetaId), "base meta must remain after flip");
        assertEquals(baseMetaBefore, table.getBaseIndexMetaId(), "base meta id must not change after flip");
        // Guard flipped: rollup is now NORMAL, no longer SHADOW.
        assertFalse(job.flipNotYetApplied(table), "flipNotYetApplied must return false after the flip");
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState(), "table state must be NORMAL after flip");
    }

    // ---- AGG rewrite projection test (Task 10 Step 1) ----------------------

    /**
     * Build an AGG range table (base sort key (k1,k2), value v SUM). Configure an AGG
     * {@link LakeRangeRollupJob} whose sort-key-ordered schema is (k2,k1,v) with sort key (k2,k1).
     * Assert the SELECT column names are backquoted (SELECT list), the target column names are raw
     * (base backquotes them once in the INSERT target list), and {@code registerShadowIndexMeta}
     * registers an AGG_KEYS rollup meta with sortKeyIdxes = [0,1] (the (k2,k1) prefix).
     */
    @Test
    public void testAggRollupRewriteProjection() throws Exception {
        // Create an AGG range table in the test DB. AGG_KEYS requires an explicit distribution clause.
        String createAgg = "create table t_agg_rollup (k1 int, k2 int, v bigint sum)\n"
                + "AGGREGATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt aggStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createAgg, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(aggStmt);
        OlapTable aggTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_agg_rollup");
        long aggBaseMetaId = aggTable.getBaseIndexMetaId();
        assertEquals(KeysType.AGG_KEYS, aggTable.getKeysType());

        // Build rollup schema (k2, k1, v) — sort-key order, with v as the AGG column.
        List<Column> aggBaseSchema = aggTable.getSchemaByIndexMetaId(aggBaseMetaId);
        Column colK1 = aggBaseSchema.stream().filter(c -> c.getName().equals("k1")).findFirst().orElseThrow();
        Column colK2 = aggBaseSchema.stream().filter(c -> c.getName().equals("k2")).findFirst().orElseThrow();
        Column colV = aggBaseSchema.stream().filter(c -> c.getName().equals("v")).findFirst().orElseThrow();
        // Rollup schema in sort-key order: k2, k1, v.
        List<Column> rollupSchema = List.of(colK2, colK1, colV);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowMetaId = GlobalStateMgr.getCurrentState().getNextId();
        LakeRangeRollupJob job = new LakeRangeRollupJob(jobId, db.getId(), aggTable.getId(),
                "t_agg_rollup", 3600_000L);
        job.setRollupSchema(rollupSchema);
        job.setRollupKeysType(KeysType.AGG_KEYS);
        // Sort key is (k2, k1) = positions 0,1 in the rollup schema.
        job.setRollupSortKeyColumns(List.of(colK2, colK1));
        job.setRollupSortKeyIdxes(List.of(0, 1));
        job.setRollupSortKeyUniqueIds(List.of(0, 1));
        job.setShadowIndex(shadowMetaId, aggBaseMetaId, "r_agg", (short) 2);

        // Assert rewriteSelectColumnNames returns backquoted SQL names (the SELECT list).
        List<String> selectNames = job.rewriteSelectColumnNames(aggTable);
        assertEquals(List.of(ParseUtil.backquote("k2"), ParseUtil.backquote("k1"), ParseUtil.backquote("v")),
                selectNames,
                "SELECT column names must be backquoted");

        // Assert rewriteTargetColumnNames returns RAW names (base backquotes them in INSERT target list).
        List<String> targetNames = job.rewriteTargetColumnNames(aggTable);
        assertEquals(List.of("k2", "k1", "v"), targetNames,
                "INSERT target column names must be raw (not backquoted)");

        // Verify the select names ARE backquoted and the target names are NOT.
        for (int i = 0; i < selectNames.size(); i++) {
            assertTrue(selectNames.get(i).startsWith("`") && selectNames.get(i).endsWith("`"),
                    "select name must be backquoted: " + selectNames.get(i));
            assertFalse(targetNames.get(i).startsWith("`"),
                    "target name must NOT be backquoted: " + targetNames.get(i));
        }

        // Assert registerShadowIndexMeta registers AGG_KEYS meta with sortKeyIdxes = [0,1].
        job.registerShadowIndexMeta(aggTable);
        MaterializedIndexMeta rollupMeta = aggTable.getIndexMetaByMetaId(shadowMetaId);
        assertNotNull(rollupMeta, "rollup meta must be registered");
        assertEquals(KeysType.AGG_KEYS, rollupMeta.getKeysType(),
                "rollup meta must carry AGG_KEYS");
        assertEquals(List.of(0, 1), rollupMeta.getSortKeyIdxes(),
                "rollup sort key idxes must be [0,1] (the (k2,k1) prefix)");
        // Base must be untouched.
        assertNotNull(aggTable.getIndexMetaByMetaId(aggBaseMetaId), "base meta must remain after rollup registration");
        assertEquals(KeysType.AGG_KEYS, aggTable.getIndexMetaByMetaId(aggBaseMetaId).getKeysType(),
                "base keys type must remain AGG_KEYS");
    }

    /**
     * Drive the FINISHED replay path twice to assert idempotency. The first replay applies the flip;
     * the second is a no-op because {@code flipNotYetApplied} returns false.
     */
    @Test
    public void testVisualiseShadowIndexReplayIsIdempotent() {
        List<Column> baseSchema = table.getSchemaByIndexMetaId(baseIndexMetaId);
        Column colK1 = baseSchema.get(0);
        Column colK2 = baseSchema.get(1);

        // Build a rollup job with the shadow in SHADOW state and a reserved commitVersion.
        LakeRangeRollupJob job = newConfiguredRollupJobWithReservedCommit(List.of(colK2, colK1));

        PhysicalPartition physicalPartition = table.getPhysicalPartitions().iterator().next();
        long visibleBeforeFlip = physicalPartition.getVisibleVersion();

        // Add shadow tablets to the inverted index (as the live job would have done in WAITING_TXN).
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        MaterializedIndex shadowIdx = physicalPartition.getLatestIndex(job.getShadowIndexMetaId());
        assertNotNull(shadowIdx, "shadow index must be present before replay");
        for (Tablet t : shadowIdx.getTablets()) {
            invertedIndex.addTablet(t.getId(),
                    new TabletMeta(dbId, tableId, physicalPartition.getId(),
                            shadowIdx.getId(), com.starrocks.thrift.TStorageMedium.HDD, true));
        }

        // Relabel as FINISHED with a finishedTimeMs so the replay branch calls visualiseShadowIndex.
        job.setJobState(AlterJobV2.JobState.FINISHED);
        job.setFinishedTimeMs(System.currentTimeMillis());
        AlterJobV2 persistCopy = job.copyForPersist();
        String text = GsonUtils.GSON.toJson(persistCopy);

        LakeRangeRollupJob replayed = (LakeRangeRollupJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);
        LakeRangeRollupJob inMemory = (LakeRangeRollupJob) GsonUtils.GSON.fromJson(text, AlterJobV2.class);

        // First replay: the flip is applied.
        assertDoesNotThrow(() -> inMemory.replay(replayed), "first replay must not throw");
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertFalse(inMemory.flipNotYetApplied(table), "after first replay flipNotYetApplied must be false");
        assertEquals(visibleBeforeFlip + 1, physicalPartition.getVisibleVersion(),
                "first replay must advance visibleVersion by one");

        // Second replay: flipNotYetApplied is false -> the flip is skipped, no version double-bump.
        assertDoesNotThrow(() -> inMemory.replay(replayed), "second replay must not throw");
        assertEquals(visibleBeforeFlip + 1, physicalPartition.getVisibleVersion(),
                "second replay must not advance visibleVersion again");
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }
}
