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
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@code AlterMVJobExecutor#visitReorderColumnsClause}: {@code ALTER MATERIALIZED VIEW ...
 * ORDER BY} on a shared-data range-distribution MV submits a {@link LakeMvSortKeyRewriteJob} (Model B --
 * the MV's base schema is unchanged, only the sort key changes) and flips the MV to SCHEMA_CHANGE,
 * mirroring the shipped {@code ALTER TABLE ... ORDER BY} reorder shape. Uses the lightweight
 * no-mincluster fixture style of {@link AlterMVJobExecutorEditLogTest} (a real {@link LakeMaterializedView}
 * constructed directly, {@code UtFrameUtils.setUpForPersistTest()} for the pseudo edit log) rather than a
 * full shared-data test cluster.
 */
public class AlterMVJobExecutorTest {
    private static final String DB_NAME = "test_alter_mv_sort_key";
    private static final long DB_ID = 20001L;
    private static final long MV_ID = 20002L;
    private static final long BASE_META_ID = 20003L;
    private static final String MV_NAME = "mv_range";

    private ConnectContext connectContext;
    private Database db;
    private MaterializedView mv;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        db = new Database(DB_ID, DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(db);

        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", IntegerType.INT);
        k1.setIsKey(true);
        k1.setUniqueId(0);
        columns.add(k1);
        Column k2 = new Column("k2", IntegerType.INT);
        k2.setIsKey(true);
        k2.setUniqueId(1);
        columns.add(k2);
        Column v1 = new Column("v1", IntegerType.INT);
        v1.setUniqueId(2);
        columns.add(v1);

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distributionInfo = new RangeDistributionInfo();
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);

        mv = new LakeMaterializedView(MV_ID, DB_ID, MV_NAME, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(BASE_META_ID, MV_NAME, columns, 0, 0, (short) columns.size(),
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(BASE_META_ID);
        db.registerTableUnlocked(mv);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static TableRef mvTableRef() {
        return new TableRef(QualifiedName.of(List.of(DB_NAME, MV_NAME)), null, NodePosition.ZERO);
    }

    @Test
    public void testAlterMvOrderBySubmitsRewriteJob() throws Exception {
        ReorderColumnsClause clause = new ReorderColumnsClause(List.of("k2", "k1"), null, null, NodePosition.ZERO);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(mvTableRef(), clause, NodePosition.ZERO);

        new AlterMVJobExecutor().process(stmt, connectContext);

        Map<Long, AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        AlterJobV2 job = jobs.values().stream()
                .filter(j -> j instanceof LakeMvSortKeyRewriteJob).findFirst().orElseThrow();
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());

        MaterializedView reloaded = (MaterializedView) db.getTable(MV_NAME);
        assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, reloaded.getState());

        LakeMvSortKeyRewriteJob rewriteJob = (LakeMvSortKeyRewriteJob) job;
        // Model B: the base schema (column set/order) is unchanged -- only the sort key is permuted.
        assertEquals(List.of("k1", "k2", "v1"), rewriteJob.getNewSchema().stream()
                .map(Column::getName).collect(Collectors.toList()));
        assertEquals(List.of("k2", "k1"), rewriteJob.getNewSortKeyColumns().stream()
                .map(Column::getName).collect(Collectors.toList()));
        // No refresh Task is registered for this MV -- must not be marked active-at-submit.
        assertFalse(rewriteJob.isRefreshWasActiveAtSubmit());
    }

    @Test
    public void testAlterMvOrderByExecutorRejectsUnknownColumn() {
        // AlterMVJobExecutor#resolveSortKey re-validates column existence independently of the earlier
        // AlterMVClauseAnalyzerVisitor gate (defense-in-depth against the schema changing between analyze
        // and execute). Calling process() directly, as this test does, skips that earlier analyzer gate
        // entirely, so it exercises resolveSortKey's own check in isolation.
        ReorderColumnsClause clause = new ReorderColumnsClause(List.of("no_such_col"), null, null, NodePosition.ZERO);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(mvTableRef(), clause, NodePosition.ZERO);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> new AlterMVJobExecutor().process(stmt, connectContext));
        assertTrue(ex.getMessage().contains("does not exist"), "expected column-not-found message, got: "
                + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByExecutorClearsUnstableUniqueIds() throws Exception {
        // A column with no stable unique id (never assigned, i.e. COLUMN_UNIQUE_ID_INIT_VALUE) makes
        // resolveSortKey fall back to idx-only sort-key resolution instead of unique ids.
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", IntegerType.INT);
        k1.setIsKey(true);
        columns.add(k1);
        Column k2 = new Column("k2", IntegerType.INT);
        k2.setIsKey(true);
        columns.add(k2);

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distributionInfo = new RangeDistributionInfo();
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        String mvName = "mv_no_unique_ids";
        long noIdBaseMetaId = BASE_META_ID + 1;
        MaterializedView noIdMv = new LakeMaterializedView(MV_ID + 1, DB_ID, mvName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        noIdMv.setIndexMeta(noIdBaseMetaId, mvName, columns, 0, 0, (short) columns.size(),
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        noIdMv.setBaseIndexMetaId(noIdBaseMetaId);
        db.registerTableUnlocked(noIdMv);

        ReorderColumnsClause clause = new ReorderColumnsClause(List.of("k2", "k1"), null, null, NodePosition.ZERO);
        TableRef ref = new TableRef(QualifiedName.of(List.of(DB_NAME, mvName)), null, NodePosition.ZERO);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(ref, clause, NodePosition.ZERO);

        new AlterMVJobExecutor().process(stmt, connectContext);

        // getAlterJobsV2() is a live map on GlobalStateMgr's singleton SchemaChangeHandler and outlives
        // this test method (unlike db/mv, which @BeforeEach replaces fresh every test) -- remove this
        // test's own job afterwards so testAlterMvOrderBySubmitsRewriteJob's unqualified
        // "the" LakeMvSortKeyRewriteJob lookup does not race with it.
        Map<Long, AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        try {
            LakeMvSortKeyRewriteJob rewriteJob = (LakeMvSortKeyRewriteJob) jobs.values().stream()
                    .filter(j -> j instanceof LakeMvSortKeyRewriteJob && j.getTableId() == noIdMv.getId())
                    .findFirst().orElseThrow();
            assertEquals(List.of("k2", "k1"), rewriteJob.getNewSortKeyColumns().stream()
                    .map(Column::getName).collect(Collectors.toList()));
            List<Integer> uniqueIds = Deencapsulation.getField(rewriteJob, "newSortKeyUniqueIds");
            assertTrue(uniqueIds == null || uniqueIds.isEmpty(),
                    "expected no sort-key unique ids when a resolved column has no stable id, got: " + uniqueIds);
        } finally {
            jobs.values().removeIf(j -> j instanceof LakeMvSortKeyRewriteJob && j.getTableId() == noIdMv.getId());
        }
    }

    @Test
    public void testAlterMvOrderByRejectsUnavailableComputeResource() {
        // resolveAndValidateComputeResource (shared by ALTER TABLE ORDER BY's buildRangeRewriteJob and
        // this MV path) rejects the DDL outright when the session's compute resource has no alive nodes.
        // WarehouseManager#isResourceAvailable short-circuits to true outside shared-data mode, so the
        // provider-level mock below only takes effect once RunMode is forced to SHARED_DATA.
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return false;
            }
        };

        ReorderColumnsClause clause = new ReorderColumnsClause(List.of("k2", "k1"), null, null, NodePosition.ZERO);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(mvTableRef(), clause, NodePosition.ZERO);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new AlterMVJobExecutor().process(stmt, connectContext));
        assertTrue(ex.getCause() instanceof DdlException);
        assertTrue(ex.getCause().getMessage().contains("no available compute nodes"),
                "expected no-available-compute-nodes message, got: " + ex.getCause().getMessage());
    }
}
