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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
    public void testFlipNotYetAppliedFalseWhenIndexAbsent() {
        // No rollup index added to any partition -> absent -> already flipped (NORMAL).
        LakeRangeRollupJob job = new LakeRangeRollupJob(10L, dbId, tableId, "t", 3600_000L);
        job.setShadowIndex(9999L, baseIndexMetaId, "r_absent", (short) 1);
        assertFalse(job.flipNotYetApplied(table));
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
}
