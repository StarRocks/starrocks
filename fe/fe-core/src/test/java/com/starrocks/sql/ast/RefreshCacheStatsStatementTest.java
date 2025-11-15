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

package com.starrocks.sql.ast;

import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

class RefreshCacheStatsStatementTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database refresh_cache_stats;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        // create table
        String createTableStr = "create table refresh_cache_stats.aaa (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 3\n" +
                "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
    }

    @Test
    void testTableRefreshCacheStats() throws Exception {
        {
            TableName tableName = new TableName("refresh_cache_stats_aaa", "aaa");
            RefreshTableCacheStatsStatement stmt = new RefreshTableCacheStatsStatement(tableName, null);
            ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                    "db refresh_cache_stats_aaa does not exist.", () -> stmt.prepare());
        }
        {
            TableName tableName = new TableName("refresh_cache_stats", "bbb");
            RefreshTableCacheStatsStatement stmt = new RefreshTableCacheStatsStatement(tableName, null);
            ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                    "table bbb does not exist.", () -> stmt.prepare());
        }
        TableName tableName = new TableName("refresh_cache_stats", "aaa");
        RefreshTableCacheStatsStatement stmt = new RefreshTableCacheStatsStatement(tableName, null);
        Map<Long, RefreshCacheStatsStatement.PartitionSnapshot> tablets = stmt.prepare();
        Assertions.assertEquals(tablets.size(), 3);
    }

}
