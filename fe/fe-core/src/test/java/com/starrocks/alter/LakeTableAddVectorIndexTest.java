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

public class LakeTableAddVectorIndexTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_add_vector_index_test";

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
        OlapTable table = createTable(
                "CREATE TABLE t_vec (" +
                        "  id BIGINT NOT NULL," +
                        "  v  ARRAY<FLOAT> NOT NULL" +
                        ") DUPLICATE KEY(id)" +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')");

        // Should not throw; before the fix this raised IllegalArgumentException from
        // Preconditions.checkArgument(newIndex.isValidIndex(), ...) because the cloud-native
        // VECTOR branch constructed Index with indexId=-1.
        alterTable(
                "ALTER TABLE t_vec ADD INDEX idx_v (v) USING VECTOR" +
                        " ('index_type'='HNSW','metric_type'='l2_distance','dim'='4'," +
                        "'is_vector_normed'='false','M'='16','efconstruction'='40')");

        // The ALTER creates a LakeTableSchemaChangeJob (async). Retrieve it and check that
        // the indexes it carries include the VECTOR index with a valid (>= 0) index_id.
        List<AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size(), "Expected exactly one pending schema change job");

        AlterJobV2 job = jobs.get(0);
        Assertions.assertInstanceOf(LakeTableSchemaChangeJob.class, job,
                "Expected a LakeTableSchemaChangeJob for cloud-native table");

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
        String json = GsonUtils.GSON.toJson(job);
        LakeTableSchemaChangeJob recovered =
                GsonUtils.GSON.fromJson(json, LakeTableSchemaChangeJob.class);

        // Extract the shadow tablet from the recovered job's partitionIndexMap.
        @SuppressWarnings("unchecked")
        Table<Long, Long, MaterializedIndex> partitionIndexMap =
                Deencapsulation.getField(recovered, "physicalPartitionIndexMap");
        Assertions.assertNotNull(partitionIndexMap, "partitionIndexMap must not be null after deserialisation");

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
        LakeTable table = createTable(
                "CREATE TABLE t_vibv (" +
                        "  id BIGINT NOT NULL," +
                        "  v  ARRAY<FLOAT> NOT NULL" +
                        ") DUPLICATE KEY(id)" +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')");

        // Capture V_snap: the visible version of the (first) physical partition before ALTER.
        PhysicalPartition pp = table.getPhysicalPartitions().iterator().next();
        long vSnap = pp.getVisibleVersion();

        // Issue the vector-index ALTER.
        alterTable(
                "ALTER TABLE t_vibv ADD INDEX idx_v (v) USING VECTOR" +
                        " ('index_type'='HNSW','metric_type'='l2_distance','dim'='4'," +
                        "'is_vector_normed'='false','M'='16','efconstruction'='40')");

        // Retrieve the pending LakeTableSchemaChangeJob.
        List<AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size(), "expected exactly one pending schema-change job");
        AlterJobV2 job = jobs.get(0);
        Assertions.assertInstanceOf(LakeTableSchemaChangeJob.class, job);

        // Access the physicalPartitionIndexMap (private field).
        @SuppressWarnings("unchecked")
        Table<Long, Long, MaterializedIndex> partitionIndexMap =
                Deencapsulation.getField(job, "physicalPartitionIndexMap");
        Assertions.assertNotNull(partitionIndexMap);

        // Find any shadow LakeTablet and assert vibv == vSnap.
        boolean foundTablet = false;
        for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                Assertions.assertInstanceOf(LakeTablet.class, shadowTablet);
                long vibv = ((LakeTablet) shadowTablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "shadow tablet vibv must equal V_snap=" + vSnap +
                                " (partition visible version at job creation), got " + vibv);
                foundTablet = true;
            }
        }
        Assertions.assertTrue(foundTablet, "expected at least one shadow tablet in the job");

        // Also verify GSON round-trip preserves the vibv value.
        String json = GsonUtils.GSON.toJson(job);
        LakeTableSchemaChangeJob recovered =
                GsonUtils.GSON.fromJson(json, LakeTableSchemaChangeJob.class);

        @SuppressWarnings("unchecked")
        Table<Long, Long, MaterializedIndex> recoveredMap =
                Deencapsulation.getField(recovered, "physicalPartitionIndexMap");
        Assertions.assertNotNull(recoveredMap);

        for (Table.Cell<Long, Long, MaterializedIndex> cell : recoveredMap.cellSet()) {
            for (Tablet shadowTablet : cell.getValue().getTablets()) {
                long vibv = ((LakeTablet) shadowTablet).getVectorIndexBuiltVersion();
                Assertions.assertEquals(vSnap, vibv,
                        "shadow tablet vibv must survive GSON round-trip, expected " + vSnap + " got " + vibv);
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
     *   <li>Assert all shadow ids have been removed from {@code getPendingTabletsForTest()}.
     * </ol>
     */
    @Test
    public void testCancelJobEvictsShadowTabletsFromScheduler() throws Exception {
        LakeTable table = createTable(
                "CREATE TABLE t_cancel_vi (" +
                        "  id BIGINT NOT NULL," +
                        "  v  ARRAY<FLOAT> NOT NULL" +
                        ") DUPLICATE KEY(id)" +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')");

        alterTable(
                "ALTER TABLE t_cancel_vi ADD INDEX idx_v (v) USING VECTOR" +
                        " ('index_type'='HNSW','metric_type'='l2_distance','dim'='4'," +
                        "'is_vector_normed'='false','M'='16','efconstruction'='40')");

        List<AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size(), "Expected exactly one pending schema change job");
        AlterJobV2 job = jobs.get(0);
        Assertions.assertInstanceOf(LakeTableSchemaChangeJob.class, job);

        // Collect shadow tablet ids from physicalPartitionIndexMap.
        @SuppressWarnings("unchecked")
        Table<Long, Long, MaterializedIndex> partitionIndexMap =
                Deencapsulation.getField(job, "physicalPartitionIndexMap");
        Assertions.assertNotNull(partitionIndexMap);

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

        // Verify they are now pending.
        for (long id : shadowTabletIds) {
            Assertions.assertTrue(scheduler.getPendingTabletsForTest().containsKey(id),
                    "Shadow tablet " + id + " must be pending before cancel");
        }

        // Cancel the job — this must evict the shadow tablets from the scheduler.
        job.cancel("test cancel");

        // All shadow ids must have been removed from the scheduler.
        for (long id : shadowTabletIds) {
            Assertions.assertFalse(scheduler.getPendingTabletsForTest().containsKey(id),
                    "Shadow tablet " + id + " must be evicted from scheduler after ALTER cancel");
        }
    }
}
