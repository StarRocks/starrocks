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
package com.starrocks.sql.analyzer;

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;
import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;
import static org.assertj.core.api.Assertions.assertThat;

public class PlannerMetaLockerTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_db").useDatabase("test_db");
    }

    @BeforeEach
    public void beforeEach() {
        BackendResourceStat.getInstance().reset();
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, 32);
    }

    @AfterEach
    public void afterEach() throws Exception {
        try {
            starRocksAssert.dropMaterializedView("test_mv");
        } catch (Exception ignored) {
        }
        try {
            DDLStmtExecutor.execute(
                    UtFrameUtils.parseStmtWithNewParser("drop resource group test_rg", connectContext),
                    connectContext);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testResourceGroupClassifierWithSyncMV() throws Exception {
        // 1. Create base table
        starRocksAssert.withTable("CREATE TABLE test_table (" +
                "k1 int NOT NULL, " +
                "k2 int NOT NULL, " +
                "v1 int SUM) " +
                "AGGREGATE KEY(k1, k2) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3 " +
                "PROPERTIES('replication_num' = '1')");

        // 2. Create sync materialized view
        String createMvSql = "create materialized view test_mv as " +
                "select k1, sum(v1) from test_table group by k1;";
        CreateMaterializedViewStmt createMvStmt = (CreateMaterializedViewStmt)
                UtFrameUtils.parseStmtWithNewParser(createMvSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .createMaterializedView(createMvStmt);
        waitingRollupJobV2Finish();

        // 3. Create resource group with database classifier
        String createRgSql = "create resource group test_rg " +
                "to (`db`='test_db') " +
                "with (" +
                "    'cpu_core_limit' = '10'," +
                "    'mem_limit' = '50%'," +
                "    'concurrency_limit' = '5'," +
                "    'type' = 'normal'" +
                ");";
        DDLStmtExecutor.execute(
                UtFrameUtils.parseStmtWithNewParser(createRgSql, connectContext),
                connectContext);

        // 4. Execute query with [_SYNC_MV_] hint
        // This triggers PlannerMetaLocker.resolveTable() with the sync MV fallback
        String querySql = "select * from test_mv [_SYNC_MV_];";
        UtFrameUtils.getPlanAndFragment(connectContext, querySql);

        // 5. Verify database ID was captured in currentSqlDbIds
        Set<Long> dbIds = connectContext.getCurrentSqlDbIds();
        long testDbId = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test_db").getId();
        assertThat(dbIds)
                .as("Database ID should be registered in currentSqlDbIds after sync MV query")
                .contains(testDbId);

        // 6. Verify resource group classifier matches
        TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr()
                .chooseResourceGroup(
                        connectContext,
                        ResourceGroupClassifier.QueryType.SELECT,
                        dbIds);

        assertThat(wg)
                .as("Resource group should not be null")
                .isNotNull();
        assertThat(wg.getName())
                .as("Sync MV query should match database classifier")
                .isEqualTo("test_rg");
        assertThat(wg.getName())
                .as("Should not fall back to default resource group")
                .isNotEqualTo(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
    }
}
