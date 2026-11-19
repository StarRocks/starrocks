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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.MockedBackend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A node that predates the lake ADD/DROP INDEX fast path ignores only_add_index / only_drop_index and runs the
 * regular schema change, which rewrites the tablet into itself and duplicates its rows. While any node of the
 * warehouse does not report the fast path, index and bloom-filter changes must use the regular schema change.
 */
public class LakeIndexFastPathCapabilityTest {
    private static final String DB_NAME = "test_lake_index_fast_path_capability";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    private static AlterJobV2 createJob(String tableName, String alterSql) throws Exception {
        starRocksAssert.withTable("create table " + tableName + " (k1 int, v1 int, v2 varchar(32),\n"
                + "index idx_v1 (v1) using bitmap)\n"
                + "duplicate key(k1) distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(stmt.getAlterClauseList(), db, table);
    }

    @Test
    public void testIndexChangesFallBackWhenANodeLacksFastPath() throws Exception {
        long nodeId = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getAllComputeNodeIds(WarehouseManager.DEFAULT_RESOURCE).get(0);
        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
        MockedBackend.reportLakeIndexFastPath = false;
        node.setSupportLakeIndexFastPath(false);
        try {
            String[][] alters = {
                    {"t_add", "alter table t_add add index idx_v2 (v2) using bitmap"},
                    {"t_drop", "alter table t_drop drop index idx_v1"},
                    {"t_bf", "alter table t_bf set ('bloom_filter_columns' = 'v2')"}};
            for (String[] alter : alters) {
                AlterJobV2 job = createJob(alter[0], alter[1]);
                assertNotNull(job, alter[1]);
                assertFalse(job instanceof LakeTableIndexFastPathJobBase, alter[1]);
            }
        } finally {
            MockedBackend.reportLakeIndexFastPath = true;
            node.setSupportLakeIndexFastPath(true);
        }
    }
}
