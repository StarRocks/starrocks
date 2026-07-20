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

public class LakeTableAddGinIndexTest {
    private static final String DB_NAME = "db_lake_add_gin_index_test";
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        Config.enable_experimental_gin = true;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void before() throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE DATABASE " + DB_NAME, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    @Test
    public void testAddGinIndexIgnoresReplicatedStorageInSharedDataMode() throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE TABLE t_gin ("
                        + "id BIGINT NOT NULL, search_word STRING NOT NULL"
                        + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'replicated_storage'='true')",
                connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);

        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "t_gin");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.enableReplicatedStorage());

        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE t_gin ADD INDEX gin_search_word (search_word) "
                        + "USING GIN('parser'='chinese', 'imp_lib'='builtin')",
                connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt);

        List<AlterJobV2> jobs = GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size());
        Assertions.assertInstanceOf(LakeTableSchemaChangeJob.class, jobs.get(0));

        @SuppressWarnings("unchecked")
        List<Index> indexes = Deencapsulation.getField(jobs.get(0), "indexes");
        Assertions.assertTrue(indexes.stream().anyMatch(index ->
                index.getIndexType() == IndexDef.IndexType.GIN
                        && index.getIndexName().equals("gin_search_word")));
    }
}
