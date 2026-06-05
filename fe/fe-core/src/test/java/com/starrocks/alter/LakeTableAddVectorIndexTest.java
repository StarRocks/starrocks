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
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.vector.VectorIndexBuildScheduler;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LakeTableAddVectorIndexTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_add_vector_index_test";

    private static final String VECTOR_INDEX_PROPS =
            "'index_type'='HNSW','metric_type'='l2_distance','dim'='4'," +
            "'is_vector_normed'='false','M'='16','efconstruction'='40'";

    @BeforeAll
    public static void setUp() throws Exception {
        Config.enable_experimental_vector = true;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void before() throws Exception {
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    // ========== Static helpers ==========

    private static LakeTable createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        com.starrocks.catalog.Database db =
                GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    /** Create a standard vector table with the given name. */
    private static LakeTable createVectorTable(String name) throws Exception {
        return createTable(
                "CREATE TABLE " + name + " (" +
                        "  id BIGINT NOT NULL," +
                        "  v  ARRAY<FLOAT> NOT NULL" +
                        ") DUPLICATE KEY(id)" +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')");
    }

    /** Issue a synchronous vector-index ALTER TABLE on the given table. */
    private static void addVectorIndex(String tableName) throws Exception {
        alterTable(
                "ALTER TABLE " + tableName + " ADD INDEX idx_v (v) USING VECTOR (" +
                        VECTOR_INDEX_PROPS + ")");
    }

    /** Create a vector table that already carries the VECTOR index at CREATE TABLE time. */
    private static LakeTable createVectorTableWithIndex(String name) throws Exception {
        return createTable(
                "CREATE TABLE " + name + " (" +
                        "  id BIGINT NOT NULL," +
                        "  v  ARRAY<FLOAT> NOT NULL," +
                        "  INDEX idx_v (v) USING VECTOR (" + VECTOR_INDEX_PROPS + ")" +
                        ") DUPLICATE KEY(id)" +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')");
    }

    /**
     * Create a vector table with two scalar key columns so the sort key can be reordered
     * (ALTER ... ORDER BY), which exercises the sort-key-only schema-change path.
     */
    private static LakeTable createVectorTableTwoKeys(String name) throws Exception {
        return createTable(
                "CREATE TABLE " + name + " (" +
                        "  id BIGINT NOT NULL," +
                        "  k  BIGINT NOT NULL," +
                        "  v  ARRAY<FLOAT> NOT NULL," +
                        "  INDEX idx_v (v) USING VECTOR (" + VECTOR_INDEX_PROPS + ")" +
                        ") DUPLICATE KEY(id, k)" +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')");
    }

    /**
     * Retrieve the single pending {@link LakeTableSchemaChangeJob} for {@code table},
     * asserting exactly one job exists and that it is a {@code LakeTableSchemaChangeJob}.
     */
    private static LakeTableSchemaChangeJob getSinglePendingJob(OlapTable table) {
        List<AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size(), "Expected exactly one pending schema-change job");
        AlterJobV2 job = jobs.get(0);
        Assertions.assertInstanceOf(LakeTableSchemaChangeJob.class, job,
                "Expected a LakeTableSchemaChangeJob for cloud-native table");
        return (LakeTableSchemaChangeJob) job;
    }

    /**
     * Return the {@code physicalPartitionIndexMap} from {@code job}, asserting it is non-null.
     */
    @SuppressWarnings("unchecked")
    private static Table<Long, Long, MaterializedIndex> getPartitionIndexMap(LakeTableSchemaChangeJob job) {
        Table<Long, Long, MaterializedIndex> map =
                Deencapsulation.getField(job, "physicalPartitionIndexMap");
        Assertions.assertNotNull(map, "physicalPartitionIndexMap must not be null");
        return map;
    }

    /** Serialize {@code job} to JSON and back with {@link GsonUtils#GSON}. */
    private static LakeTableSchemaChangeJob gsonRoundTrip(LakeTableSchemaChangeJob job) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), LakeTableSchemaChangeJob.class);
    }

    // ========== Tests ==========

    /**
     * R3 fix: ALTER TABLE ... ADD INDEX ... USING VECTOR on a cloud-native (lake) table must
     * assign a valid index_id (>= 0). Before the fix, processAddIndex created the Index with
     * indexId=-1 for cloud-native VECTOR, causing checkArgument(isValidIndex()) to throw.
     *
     * After the fix, no exception is thrown and the LakeTableSchemaChangeJob's index list
     * contains the VECTOR index with a valid index_id.
     */
    @Test
    public void testAddVectorIndexAssignsIndexId() throws Exception {
        OlapTable table = createVectorTable("t_vec");

        // Should not throw; before the fix this raised IllegalArgumentException from
        // Preconditions.checkArgument(newIndex.isValidIndex(), ...) because the cloud-native
        // VECTOR branch constructed Index with indexId=-1.
        addVectorIndex("t_vec");

        // The ALTER creates a LakeTableSchemaChangeJob (async). Retrieve it and check that
        // the indexes it carries include the VECTOR index with a valid (>= 0) index_id.
        LakeTableSchemaChangeJob job = getSinglePendingJob(table);

        // Access the private 'indexes' field stored in the job (set by withAlterIndexInfo).
        @SuppressWarnings("unchecked")
        List<Index> jobIndexes = Deencapsulation.getField(job, "indexes");
        Assertions.assertNotNull(jobIndexes, "Job indexes must not be null");

        Index vi = jobIndexes.stream()
                .filter(i -> i.getIndexType() == IndexDef.IndexType.VECTOR)
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(vi, "VECTOR index must be present in the schema change job");
        Assertions.assertTrue(vi.getIndexId() >= 0,
                "cloud-native VECTOR index must have a valid index_id (>= 0), got: " + vi.getIndexId());
    }

    /**
     * Verify that GSON preserves a shadow tablet's {@code vectorIndexBuiltVersion} (vibv) through a
     * serialize → deserialize round-trip.
     *
     * <p>This test does NOT require the full schema-change infrastructure: it builds a
     * {@link LakeTableSchemaChangeJob} directly, plants a {@link LakeTablet} with vibv=7 inside it,
     * serialises with {@code GsonUtils.GSON}, deserialises, and asserts the recovered vibv == 7.
     *
     * <p>The chain that makes this work:
     * <pre>
     *   LakeTableSchemaChangeJob
     *     @SerializedName("partitionIndexMap") physicalPartitionIndexMap
     *       → MaterializedIndex
     *           @SerializedName("tablets") List&lt;Tablet&gt;
     *             → LakeTablet (via RuntimeTypeAdapterFactory "clazz":"LakeTablet")
     *                 @SerializedName("vibv") vectorIndexBuiltVersion
     * </pre>
     */
    @Test
    public void testGsonRoundTripPreservesVibv() {
        // Build a minimal job carrying one shadow tablet with vibv = 7.
        LakeTableSchemaChangeJob job = new LakeTableSchemaChangeJob(1L, 2L, 3L, "t", 3600_000L);

        LakeTablet shadowTablet = new LakeTablet(42L);
        shadowTablet.setVectorIndexBuiltVersion(7L);

        MaterializedIndex shadowIndex =
                new MaterializedIndex(10L, MaterializedIndex.IndexState.SHADOW, 0L);
        TabletMeta meta = new TabletMeta(2L, 3L, 100L, 10L, TStorageMedium.SSD, true);
        shadowIndex.addTablet(shadowTablet, meta);

        // physicalPartitionId=100, shadowIndexMetaId=10
        job.addPartitionShadowIndex(100L, 10L, shadowIndex);

        // GSON round-trip
        LakeTableSchemaChangeJob recovered = gsonRoundTrip(job);

        // Extract the shadow tablet from the recovered job's partitionIndexMap.
        Table<Long, Long, MaterializedIndex> partitionIndexMap = getPartitionIndexMap(recovered);

        MaterializedIndex recoveredIndex = partitionIndexMap.get(100L, 10L);
        Assertions.assertNotNull(recoveredIndex, "shadow MaterializedIndex must survive round-trip");

        List<Tablet> tablets = recoveredIndex.getTablets();
        Assertions.assertEquals(1, tablets.size(), "exactly one shadow tablet expected");

        Tablet t = tablets.get(0);
        Assertions.assertInstanceOf(LakeTablet.class, t,
                "tablet must deserialise as LakeTablet (RuntimeTypeAdapterFactory must be active)");
        Assertions.assertEquals(7L, ((LakeTablet) t).getVectorIndexBuiltVersion(),
                "vibv must survive GSON round-trip: expected 7");
    }

    /**
     * Verify that when a vector-index ALTER TABLE schema change job is created, the shadow
     * tablet's {@code vectorIndexBuiltVersion} (vibv) is set to the partition's visible version
     * (V_snap) and that vibv survives a GSON round-trip (FE-failover durability).
     *
     * <p>Before the implementation in {@code LakeTableSchemaChangeJob.runPendingJob()}, vibv
     * defaults to 0, so the assertion on {@code vibv == visibleVersion} will FAIL.
     */
    @Test
    public void testShadowTabletVibvSetToVsnap() throws Exception {
        // Create a lake table with an ARRAY<FLOAT> column for the vector index.
        LakeTable table = createVectorTable("t_vibv");

        // Capture V_snap: the visible version of the (first) physical partition before ALTER.
        PhysicalPartition pp = table.getPhysicalPartitions().iterator().next();
        long vSnap = pp.getVisibleVersion();

        // Issue the vector-index ALTER.
        addVectorIndex("t_vibv");

        // Retrieve the pending LakeTableSchemaChangeJob.
        LakeTableSchemaChangeJob job = getSinglePendingJob(table);

        // Access the physicalPartitionIndexMap (private field).
        Table<Long, Long, MaterializedIndex> partitionIndexMap = getPartitionIndexMap(job);

        // Find any shadow LakeTablet and assert vibv == vSnap.
        Assertions.assertFalse(partitionIndexMap.cellSet().isEmpty(),
                "physicalPartitionIndexMap must contain at least one shadow index");
        for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                Assertions.assertInstanceOf(LakeTablet.class, shadowTablet);
                long vibv = ((LakeTablet) shadowTablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "shadow tablet vibv must equal V_snap=" + vSnap +
                                " (partition visible version at job creation), got " + vibv);
            }
        }

        // Also verify GSON round-trip preserves the vibv value.
        LakeTableSchemaChangeJob recovered = gsonRoundTrip(job);
        Table<Long, Long, MaterializedIndex> recoveredMap = getPartitionIndexMap(recovered);

        Assertions.assertFalse(recoveredMap.cellSet().isEmpty(),
                "recovered physicalPartitionIndexMap must not be empty");
        for (Table.Cell<Long, Long, MaterializedIndex> cell : recoveredMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                long vibv = ((LakeTablet) shadowTablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "shadow tablet vibv must survive GSON round-trip, expected " + vSnap + " got " + vibv);
            }
        }
    }

    /**
     * Regression: a rewrite that does NOT change indexes (here, ADD a key column → non-fast
     * schema change) on a table that already has a VECTOR index must still stamp the shadow
     * tablets' vibv = V_snap.
     *
     * <p>The conversion writer force-inline-builds the vector index over all existing data
     * regardless of whether this ALTER touches indexes (see DirectSchemaChange in
     * be/src/storage/lake/schema_change.cpp). If vibv were left 0 for this case (the prior
     * behavior, which gated stamping on {@code hasIndexChanged}), the async
     * {@link VectorIndexBuildScheduler} would redundantly rebuild the entire existing dataset
     * after the ALTER.
     *
     * <p>This test FAILS before the fix (vibv == 0) and PASSES after (vibv == V_snap).
     */
    @Test
    public void testNonIndexChangeRewriteStampsVibvWhenTableHasVectorIndex() throws Exception {
        // Table already carries the VECTOR index, so this ALTER does not change indexes.
        LakeTable table = createVectorTableWithIndex("t_vibv_rewrite");

        PhysicalPartition pp = table.getPhysicalPartitions().iterator().next();
        long vSnap = pp.getVisibleVersion();

        // Adding a KEY column forces a real (non-fast) lake schema change with shadow tablets,
        // without altering the index set (hasIndexChanged == false).
        alterTable("ALTER TABLE t_vibv_rewrite ADD COLUMN k2 BIGINT KEY DEFAULT \"0\" AFTER id");

        LakeTableSchemaChangeJob job = getSinglePendingJob(table);
        Table<Long, Long, MaterializedIndex> partitionIndexMap = getPartitionIndexMap(job);

        Assertions.assertFalse(partitionIndexMap.cellSet().isEmpty(),
                "physicalPartitionIndexMap must contain at least one shadow index");
        for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                Assertions.assertInstanceOf(LakeTablet.class, shadowTablet);
                long vibv = ((LakeTablet) shadowTablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "shadow tablet vibv must equal V_snap=" + vSnap +
                                " even though this ALTER does not change indexes, got " + vibv);
            }
        }
    }

    /**
     * A sort-key change (ALTER ... ORDER BY → SortedSchemaChange) on a table with a VECTOR index
     * rewrites the existing data through the conversion writer with an inline vector-index build,
     * so the shadow tablets are stamped with vibv = V_snap to let the async build scheduler skip
     * the already-built data. Verifies the shadow tablets carry vibv = V_snap.
     */
    @Test
    public void testSortKeyChangeStampsVibvWhenTableHasVectorIndex() throws Exception {
        LakeTable table = createVectorTableTwoKeys("t_vibv_sortkey");

        PhysicalPartition pp = table.getPhysicalPartitions().iterator().next();
        long vSnap = pp.getVisibleVersion();

        // Sort-key change routes through createJobForProcessModifySortKeyColumn (indexes == null).
        alterTable("ALTER TABLE t_vibv_sortkey ORDER BY (k, id)");

        LakeTableSchemaChangeJob job = getSinglePendingJob(table);
        Table<Long, Long, MaterializedIndex> partitionIndexMap = getPartitionIndexMap(job);

        Assertions.assertFalse(partitionIndexMap.cellSet().isEmpty(),
                "physicalPartitionIndexMap must contain at least one shadow index");
        for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                Assertions.assertInstanceOf(LakeTablet.class, shadowTablet);
                long vibv = ((LakeTablet) shadowTablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "sort-key-change shadow tablet vibv must equal V_snap=" + vSnap +
                                " (indexes==null → fallback to table.getIndexes()), got " + vibv);
            }
        }
    }

    /**
     * Verify that cancelling a vector-ADD ALTER TABLE schema-change job evicts its shadow
     * tablets from {@link VectorIndexBuildScheduler}.
     *
     * <p>Steps:
     * <ol>
     *   <li>Create a lake table and issue a vector ADD ALTER (creates a LakeTableSchemaChangeJob in PENDING state).
     *   <li>Collect the shadow tablet ids from the job's {@code physicalPartitionIndexMap}.
     *   <li>Manually register those ids in the scheduler (simulating what recoveryScan would do during RUNNING).
     *   <li>Cancel the job via {@code job.cancel("test")}.
     *   <li>Assert all shadow ids have been removed from the scheduler's {@code pendingTablets} map.
     * </ol>
     */
    @Test
    public void testCancelJobEvictsShadowTabletsFromScheduler() throws Exception {
        LakeTable table = createVectorTable("t_cancel_vi");
        addVectorIndex("t_cancel_vi");

        LakeTableSchemaChangeJob job = getSinglePendingJob(table);

        // Collect shadow tablet ids from physicalPartitionIndexMap.
        Table<Long, Long, MaterializedIndex> partitionIndexMap = getPartitionIndexMap(job);

        List<Long> shadowTabletIds = new ArrayList<>();
        for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                shadowTabletIds.add(shadowTablet.getId());
            }
        }
        Assertions.assertFalse(shadowTabletIds.isEmpty(),
                "Expected at least one shadow tablet in the job");

        // Simulate what recoveryScan / publish would do: register the shadow tablets in the scheduler.
        VectorIndexBuildScheduler scheduler =
                GlobalStateMgr.getCurrentState().getVectorIndexBuildScheduler();
        Assertions.assertNotNull(scheduler, "VectorIndexBuildScheduler must be registered in GlobalStateMgr");
        for (long id : shadowTabletIds) {
            scheduler.addPendingTablet(id, 1L, false);
        }

        // Live reference to the scheduler's private pending map, so the asserts below observe eviction.
        Map<Long, ?> pendingTablets = Deencapsulation.getField(scheduler, "pendingTablets");

        // Verify they are now pending.
        for (long id : shadowTabletIds) {
            Assertions.assertTrue(pendingTablets.containsKey(id),
                    "Shadow tablet " + id + " must be pending before cancel");
        }

        // Cancel the job — this must evict the shadow tablets from the scheduler.
        job.cancel("test cancel");

        // All shadow ids must have been removed from the scheduler.
        for (long id : shadowTabletIds) {
            Assertions.assertFalse(pendingTablets.containsKey(id),
                    "Shadow tablet " + id + " must be evicted from scheduler after ALTER cancel");
        }
    }

    /**
     * Verify that the FE-failover/restart replay path recovers shadow tablet
     * {@code vectorIndexBuiltVersion} (vibv) into live catalog state.
     *
     * <p>This tests the actual replay path, not just GSON round-trip in isolation.
     * The scenario modelled is:
     * <ol>
     *   <li>FE leader runs {@code ALTER TABLE … ADD INDEX … USING VECTOR}, creating a
     *       {@link LakeTableSchemaChangeJob} in PENDING state with shadow tablets whose
     *       {@code vibv = V_snap} (baseline covered by Task 5).</li>
     *   <li>The job advances to WAITING_TXN and its state is persisted to the edit log
     *       via {@link GsonUtils#GSON} serialisation.</li>
     *   <li>FE restarts.  The new leader replays the edit-log entry by deserialising the
     *       JSON back to a {@link LakeTableSchemaChangeJob} and calling
     *       {@code SchemaChangeHandler.replayAlterJobV2(deserialisedJob)}.</li>
     *   <li>Because no in-memory job exists yet, the handler calls
     *       {@code deserialisedJob.replay(deserialisedJob)}, which — for WAITING_TXN state
     *       — invokes {@code addShadowIndexToCatalog}, attaching the shadow
     *       {@link MaterializedIndex} (together with its deserialized LakeTablets) to the
     *       physical partition.</li>
     *   <li>After replay the physical partition's shadow indexes must contain tablets with
     *       {@code vibv == V_snap} (NOT 0), so the async {@code VectorIndexBuildScheduler}
     *       can correctly skip already-built rowsets across the failover.</li>
     * </ol>
     *
     * <p>Mechanically:
     * <pre>
     *   1. createTable + alterTable  →  PENDING job, physicalPartitionIndexMap has shadow
     *      LakeTablets with vibv = vSnap.
     *   2. GSON round-trip of the job (simulate edit-log persist + reload).
     *   3. Set deserialisedJob.state = WAITING_TXN, watershedTxnId = 1 (any non-(-1) value).
     *   4. Clear the handler's in-memory job map (simulate fresh FE — no prior in-memory job).
     *   5. replayAlterJobV2(deserialisedJob)  →  addShadowIndexToCatalog  →  shadow index
     *      added to physical partition.
     *   6. Assert: partition shadow indexes have tablets with vibv == vSnap (not 0).
     * </pre>
     */
    @Test
    public void testReplayRecoversShadowTabletVibvIntoLiveCatalog() throws Exception {
        // ---- Step 1: create table + issue vector-index ALTER ----
        LakeTable table = createVectorTable("t_replay_vibv");

        // Capture V_snap before ALTER (visible version of the first physical partition).
        PhysicalPartition pp = table.getPhysicalPartitions().iterator().next();
        long vSnap = pp.getVisibleVersion();

        addVectorIndex("t_replay_vibv");

        // Retrieve the in-memory PENDING job and sanity-check it has shadow tablets.
        LakeTableSchemaChangeJob pendingJob = getSinglePendingJob(table);

        // ---- Step 2: GSON round-trip to simulate edit-log persist + reload ----
        LakeTableSchemaChangeJob deserialisedJob = gsonRoundTrip(pendingJob);

        // Confirm the deserialisedJob carries shadow tablets (inherited from GSON round-trip).
        Table<Long, Long, MaterializedIndex> desMap = getPartitionIndexMap(deserialisedJob);
        Assertions.assertFalse(desMap.isEmpty(),
                "deserialisedJob physicalPartitionIndexMap must not be empty");

        // ---- Step 3: advance deserialisedJob to WAITING_TXN ----
        // In a real restart the WAITING_TXN edit-log entry is the one replayed; here we
        // simulate it by advancing the deserialisedJob's state + setting watershedTxnId.
        deserialisedJob.setJobState(AlterJobV2.JobState.WAITING_TXN);
        Deencapsulation.setField(deserialisedJob, "watershedTxnId", 1L);

        // ---- Step 4: clear the handler's job map (simulate fresh FE with no prior job) ----
        GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().clearJobs();

        // ---- Step 5: drive the replay — this is the actual failover path ----
        // replayAlterJobV2 finds no existing job → calls deserialisedJob.replay(deserialisedJob)
        // → WAITING_TXN branch → addShadowIndexToCatalog → shadow index attached to partition.
        GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().replayAlterJobV2(deserialisedJob);

        // ---- Step 6: assert vibv == vSnap in live catalog shadow indexes ----
        // The physical partition must now have at least one shadow index whose tablets carry vibv.
        List<MaterializedIndex> shadowIndexes = pp.getLatestMaterializedIndices(IndexExtState.SHADOW);
        Assertions.assertFalse(shadowIndexes.isEmpty(),
                "Physical partition must have at least one shadow index after replay");

        for (MaterializedIndex shadowIdx : shadowIndexes) {
            List<Tablet> tablets = shadowIdx.getTablets();
            Assertions.assertFalse(tablets.isEmpty(),
                    "Shadow index must contain at least one tablet after replay");
            for (Tablet tablet : tablets) {
                Assertions.assertInstanceOf(LakeTablet.class, tablet,
                        "shadow tablet must be a LakeTablet after replay");
                long vibv = ((LakeTablet) tablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "shadow tablet vibv must equal V_snap=" + vSnap +
                                " after replay (FE-failover recovery), got " + vibv);
            }
        }
    }
}
