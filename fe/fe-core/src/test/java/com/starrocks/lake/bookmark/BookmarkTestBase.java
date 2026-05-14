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

package com.starrocks.lake.bookmark;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Boots one SHARED_DATA mini-cluster per JVM and provides DDL helpers for the
 * Tier 2 bookmark tests. Each test creates a freshly-named table so the
 * (dbId, tableId) it operates on is unique.
 */
public abstract class BookmarkTestBase {

    protected static final String DB_NAME = "bookmark_test_db";
    protected static long dbId;
    protected static ConnectContext connectContext;

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    @BeforeAll
    public static void beforeBase() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        String createDb = "create database " + DB_NAME + ";";
        CreateDbStmt stmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDb, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(stmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        dbId = db.getId();
        connectContext.setDatabase(DB_NAME);
    }

    /** Create a 2-partition RANGE LakeTable with a fresh, unique name. Returns the table id. */
    protected long createDefaultTable() throws Exception {
        String name = "t_" + TABLE_COUNTER.getAndIncrement();
        String ddl = "CREATE TABLE " + name + " (\n"
                + "    k bigint NOT NULL,\n"
                + "    dt date NOT NULL,\n"
                + "    v bigint\n"
                + ") DUPLICATE KEY(k, dt)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "    PARTITION p1 VALUES LESS THAN ('2024-02-01'),\n"
                + "    PARTITION p2 VALUES LESS THAN ('2024-03-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\");";
        return createTable(ddl);
    }

    /** Custom DDL escape hatch — DDL must be a single CREATE TABLE statement. */
    protected long createTable(String ddl) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        return db.getTable(stmt.getTableName()).getId();
    }

    /** ALTER TABLE ... ADD PARTITION p VALUES LESS THAN ('range') */
    protected void addPartition(long tableId, String partitionName, String range) throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        String ddl = "ALTER TABLE " + table.getName() + " ADD PARTITION " + partitionName
                + " VALUES LESS THAN ('" + range + "');";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }
}
