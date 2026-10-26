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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeTableIndexColumnIdentityTest {
    private static final String DB_NAME = "test_lake_index_column_identity";
    private static ConnectContext connectContext;
    private LakeTable table;
    private Database db;

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
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE TABLE t (k BIGINT, v BIGINT, other BIGINT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')", connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        table = (LakeTable) db.getTable("t");
    }

    @AfterEach
    public void after() throws Exception {
        handler().clearJobs();
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    private static SchemaChangeHandler handler() {
        return GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    private <T extends AlterJobV2> T getJob(Class<T> jobClass) {
        List<AlterJobV2> jobs = handler().getUnfinishedAlterJobV2ByTableId(table.getId());
        assertEquals(1, jobs.size());
        return assertInstanceOf(jobClass, jobs.get(0));
    }

    private TAlterTabletReqV2 requestFor(LakeTableAddIndexJob job) {
        long indexMetaId = table.getBaseIndexMetaId();
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(indexMetaId);
        TTabletSchema readSchema = SchemaInfo.fromMaterializedIndex(table, indexMetaId, meta).toTabletSchema();
        AlterReplicaTask task = AlterReplicaTask.alterLakeTablet(1, db.getId(), table.getId(), 1,
                indexMetaId, 1, 1, 1, job.getJobId(), 1, null, readSchema);
        job.populateAlterRequest(task, indexMetaId, meta, table);
        return task.toThrift();
    }

    private static void assertIndexedColumn(TAlterTabletReqV2 request, Column target) {
        assertTrue(request.isOnly_add_index());
        assertEquals(1, request.getIndexes_to_addSize());
        assertEquals(List.of(target.getColumnId().getId()), request.getIndexes_to_add().get(0).getColumns());
        TColumn wireColumn = request.getBase_tablet_read_schema().getColumns().stream()
                .filter(column -> column.getColumn_name().equals(target.getColumnId().getId()))
                .findFirst().orElseThrow();
        assertEquals(target.getUniqueId(), wireColumn.getCol_unique_id());
    }

    @Test
    public void testRenameThenAddBitmapWithReusedName() throws Exception {
        Column original = table.getColumn("v");
        alterTable("ALTER TABLE t RENAME COLUMN v TO vr");
        alterTable("ALTER TABLE t RENAME COLUMN other TO v");
        assertEquals(original.getColumnId(), table.getColumn("vr").getColumnId());

        alterTable("ALTER TABLE t ADD INDEX idx (vr) USING BITMAP");
        LakeTableAddIndexJob job = getJob(LakeTableAddIndexJob.class);
        assertEquals(List.of(original.getColumnId()), job.getNewIndexes().get(0).getColumns());
        assertIndexedColumn(requestFor(job), original);
        assertEquals(List.of("vr"), job.getIndexesToAdd().get(0).getColumns());
        assertIndexedColumn(requestFor((LakeTableAddIndexJob) job.copyForPersist()), original);
        assertIndexedColumn(requestFor(job), original);
        assertEquals(List.of("vr"), job.getIndexesToAdd().get(0).getColumns());
    }

    @Test
    public void testRenameThenAddAndDropBloomFilter() throws Exception {
        Column original = table.getColumn("v");
        alterTable("ALTER TABLE t RENAME COLUMN v TO vr");
        alterTable("ALTER TABLE t RENAME COLUMN other TO v");
        alterTable("ALTER TABLE t SET ('bloom_filter_columns'='vr')");
        LakeTableAddIndexJob addJob = getJob(LakeTableAddIndexJob.class);
        assertIndexedColumn(requestFor(addJob), original);

        // Exercise the catalog flip separately from the BE publish lifecycle.
        addJob.applyCatalogMutation(table);
        assertEquals(Set.of(original.getColumnId()), table.getBfColumnIds());
        TTabletSchema schema = SchemaInfo.fromMaterializedIndex(table, table.getBaseIndexMetaId(),
                table.getIndexMetaByMetaId(table.getBaseIndexMetaId())).toTabletSchema();
        assertEquals(List.of(original.getColumnId().getId()), schema.getColumns().stream()
                .filter(TColumn::isIs_bloom_filter_column).map(TColumn::getColumn_name).toList());

        handler().clearJobs();
        table.setState(OlapTable.OlapTableState.NORMAL);
        alterTable("ALTER TABLE t SET ('bloom_filter_columns'='')");
        LakeTableDropIndexJob dropJob = getJob(LakeTableDropIndexJob.class);
        assertEquals(1, dropJob.getDropInfos().size());
        assertEquals(original.getUniqueId(), dropJob.getDropInfos().get(0).getCol_unique_id());
        dropJob.applyCatalogMutation(table);
        assertNull(table.getBfColumnIds());
    }
}
