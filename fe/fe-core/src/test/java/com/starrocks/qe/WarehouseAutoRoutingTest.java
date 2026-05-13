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

package com.starrocks.qe;

import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WarehouseAutoRoutingTest extends StarRocksTestBase {
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
        ConnectContext.threadLocalInfo.set(ctx);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.threadLocalInfo.remove();
        Config.enable_warehouse_auto_routing = false;
        Config.warehouse_route_write = "default_warehouse";
    }

    private StatementBase parse(String sql) throws Exception {
        return SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());
    }

    // ---- isWriteStmt classification tests ----

    @Test
    public void testInsertIsWriteStmt() throws Exception {
        StatementBase stmt = parse("INSERT INTO t1 SELECT * FROM t0");
        assertTrue(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testSelectIsNotWriteStmt() throws Exception {
        StatementBase stmt = parse("SELECT * FROM t0");
        assertFalse(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testSelectWithCTEIsNotWriteStmt() throws Exception {
        StatementBase stmt = parse("WITH cte AS (SELECT 1) SELECT * FROM cte");
        assertFalse(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testCreateTableAsSelectIsWriteStmt() throws Exception {
        StatementBase stmt = parse("CREATE TABLE t_new AS SELECT * FROM t0");
        assertTrue(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testAnalyzeIsWriteStmt() throws Exception {
        StatementBase stmt = parse("ANALYZE TABLE t0");
        assertTrue(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testShowTablesIsNotWriteStmt() throws Exception {
        StatementBase stmt = parse("SHOW TABLES");
        assertFalse(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testDeleteIsWriteStmt() throws Exception {
        StatementBase stmt = parse("DELETE FROM t0 WHERE v1 = 1");
        assertTrue(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testUpdateIsWriteStmt() throws Exception {
        StatementBase stmt = parse("UPDATE t0 SET v2 = 1 WHERE v1 = 1");
        assertTrue(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testExplainIsNotWriteStmt() throws Exception {
        StatementBase stmt = parse("EXPLAIN SELECT * FROM t0");
        assertFalse(StmtExecutor.isWriteStmt(stmt));
    }

    @Test
    public void testDescribeIsNotWriteStmt() throws Exception {
        StatementBase stmt = parse("DESCRIBE t0");
        assertFalse(StmtExecutor.isWriteStmt(stmt));
    }

    // ---- Config default tests ----

    @Test
    public void testAutoRoutingDisabledByDefault() {
        assertFalse(Config.enable_warehouse_auto_routing);
    }

    @Test
    public void testDefaultWriteWarehouse() {
        assertTrue(Config.warehouse_route_write.equals("default_warehouse"));
    }
}
