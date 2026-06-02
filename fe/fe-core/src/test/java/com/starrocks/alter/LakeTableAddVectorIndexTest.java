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

import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
