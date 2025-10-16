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

package com.starrocks.lake;

import com.starrocks.catalog.TableName;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.RefreshTableCacheStatsStatement;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CacheStatsExecutorTest {
    private static ConnectContext connectContext;

    @Mocked
    private LakeService lakeService;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database cache_stats_executor;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        // create table
        String createTableStr = "create table cache_stats_executor.aaa (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 3\n" +
                "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
    }

    @Test
    void testBasic() throws Exception {
        TableName tableName = new TableName("cache_stats_executor", "aaa");
        RefreshTableCacheStatsStatement stmt = new RefreshTableCacheStatsStatement(tableName, null);

        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };
        new MockUp<StarOSAgent>() {
            @Mock
            public long getPrimaryComputeNodeIdByShard(long shardId, long workerGroupId) {
                return 10001L;
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return new ComputeNode(10001L, "127.0.0.1", 10002);
            }
        };
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };

        new MockUp<RefreshTableCacheStatsStatement>() {
            @Mock
            public ShowResultSet getResult() {
                return new ShowResultSet(null, null);
            }
        };
        new MockUp<WarehouseComputeResource>() {
            @Mock
            public long getWorkerGroupId() {
                return 20002;
            }
        };
        CacheStatsExecutor.execute(stmt, connectContext);
    }
}
