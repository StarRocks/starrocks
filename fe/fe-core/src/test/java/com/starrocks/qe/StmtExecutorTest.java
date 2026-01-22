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
<<<<<<< HEAD
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.server.GlobalStateMgr;
=======
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
>>>>>>> e03788af8a ([Enhancement] Add table-level table_query_timeout. (#67547))
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StmtExecutorTest {

    @Test
    public void testIsForwardToLeader(@Mocked GlobalStateMgr state) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.isInTransferringToLeader();
                times = 1;
                result = true;

                state.getSqlParser();
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                times = 2;
                result = false;
                result = true;
            }
        };

        Assertions.assertFalse(new StmtExecutor(new ConnectContext(),
                SqlParser.parseSingleStatement("show frontends", SqlModeHelper.MODE_DEFAULT)).isForwardToLeader());
    }

    @Test
    public void testForwardExplicitTxnSelectOnFollower(@Mocked GlobalStateMgr state,
                                                       @Mocked ConnectContext ctx) {
        StatementBase stmt;
        MysqlSerializer serializer = MysqlSerializer.newInstance();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.getSqlParser();
                minTimes = 0;
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                minTimes = 0;
                result = false;

                state.isInTransferringToLeader();
                minTimes = 0;
                result = false;

                ctx.getSerializer();
                minTimes = 0;
                result = serializer;

                ctx.getTxnId();
                minTimes = 0;
                result = 1L;

                ctx.isQueryStmt((StatementBase) any);
                minTimes = 0;
                result = true;
            }
        };

        // Parse after expectations to ensure GlobalStateMgr.getSqlParser() is properly mocked
        stmt = SqlParser.parseSingleStatement("select 1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        Assertions.assertTrue(executor.isForwardToLeader());
    }

    @Test
    public void testExecType() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);

        StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Query", executor.getExecType());
        Assertions.assertFalse(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("insert into t1 select * from t2", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Insert", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("create table t1 as select * from t2", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Insert", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("update t1 set k1 = 1 where k2 = 1", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Update", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("delete from t1 where k2 = 1", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Delete", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());
    }

    @Test
    public void testExecTimeout() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);

        {
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(ctx.getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement("analyze table t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(Config.statistic_collect_query_timeout, executor.getExecTimeout());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement("create table t2 as select * from t1",
                    SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(ctx.getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());
        }
    }

    @Test
    public void testExecTimeoutWithTableQueryTimeout() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);

        // Create test database
        String dbName = "test_table_timeout_db";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        // Create table without table_query_timeout
        String createTableStmt1 = "CREATE TABLE `t1` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt1);

        // Create table with table_query_timeout = 120
        String createTableStmt2 = "CREATE TABLE `t2` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"120\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt2);

        // Create table with table_query_timeout = 600 (greater than cluster timeout 300)
        String createTableStmt3 = "CREATE TABLE `t3` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"600\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt3);

        // Create table with table_query_timeout = 60
        String createTableStmt4 = "CREATE TABLE `t4` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"60\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt4);

        // Test 1: User not explicitly set session timeout, table has no table_query_timeout
        // Should use cluster timeout (default 300)
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            int defaultTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            Assertions.assertEquals(defaultTimeout, executor.getExecTimeout());
        }

        // Test 2: User not explicitly set session timeout, table has table_query_timeout = 120
        // Should use table_query_timeout (120)
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(120, executor.getExecTimeout());
            // Verify table timeout info
            Assertions.assertNotNull(executor.getTableQueryTimeoutInfo());
            Assertions.assertEquals("test_table_timeout_db.t2", executor.getTableQueryTimeoutInfo().first);
            Assertions.assertEquals(120, executor.getTableQueryTimeoutInfo().second.intValue());
        }

        // Test 3: User not explicitly set session timeout, table has table_query_timeout = 600 (greater than cluster timeout)
        // Should use table_query_timeout (600) - this is the new behavior
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t3", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(600, executor.getExecTimeout());
            // Verify table timeout info
            Assertions.assertNotNull(executor.getTableQueryTimeoutInfo());
            Assertions.assertEquals("test_table_timeout_db.t3", executor.getTableQueryTimeoutInfo().first);
            Assertions.assertEquals(600, executor.getTableQueryTimeoutInfo().second.intValue());
        }

        // Test 4: User explicitly set session timeout = 200, table has table_query_timeout = 120
        // Should use session timeout (200) - session timeout has higher priority
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            testCtx.getSessionVariable().setQueryTimeoutS(200);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(200, executor.getExecTimeout());
        }

        // Test 5: User explicitly set session timeout = 100, table has table_query_timeout = 600
        // Should use session timeout (100) - session timeout has higher priority
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            testCtx.getSessionVariable().setQueryTimeoutS(100);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t3", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(100, executor.getExecTimeout());
        }

        // Test 6: Multiple tables with different table_query_timeout, should use minimum
        // t2 has 120, t4 has 60, should use 60
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2, t4", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(60, executor.getExecTimeout());
            // Verify table timeout info points to the table with minimum timeout
            Assertions.assertNotNull(executor.getTableQueryTimeoutInfo());
            Assertions.assertEquals(60, executor.getTableQueryTimeoutInfo().second.intValue());
        }

        // Test 7: Test getTableQueryTimeoutInfo when no table timeout is set
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            executor.getExecTimeout(); // Initialize timeout
            Assertions.assertNull(executor.getTableQueryTimeoutInfo());
        }

        // Test 8: Test getTableQueryTimeoutInfo when table timeout is set
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            executor.getExecTimeout(); // Initialize timeout
            Pair<String, Integer> timeoutInfo = executor.getTableQueryTimeoutInfo();
            Assertions.assertNotNull(timeoutInfo);
            Assertions.assertEquals("test_table_timeout_db.t2", timeoutInfo.first);
            Assertions.assertEquals(120, timeoutInfo.second.intValue());
        }

        // Test 9: Test with non-QueryStatement (should use session timeout)
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            // Use a non-query statement like SHOW
            StatementBase stmt = SqlParser.parseSingleStatement("SHOW TABLES", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            int timeout = executor.getExecTimeout();
            int defaultTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            Assertions.assertEquals(defaultTimeout, timeout);
            Assertions.assertNull(executor.getTableQueryTimeoutInfo());
        }

        // Test 10: Test getTableQueryTimeoutInfo coverage (lines 685, 687, 688)
        // This test ensures both return paths of getTableQueryTimeoutInfo are covered
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            
            // Test with table timeout set
            StatementBase stmt1 = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt1, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor1 = new StmtExecutor(testCtx, stmt1);
            executor1.getExecTimeout();
            Pair<String, Integer> info1 = executor1.getTableQueryTimeoutInfo();
            Assertions.assertNotNull(info1);
            Assertions.assertEquals("test_table_timeout_db.t2", info1.first);
            Assertions.assertEquals(120, info1.second.intValue());
            
            // Test without table timeout
            StatementBase stmt2 = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt2, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor2 = new StmtExecutor(testCtx, stmt2);
            executor2.getExecTimeout();
            Assertions.assertNull(executor2.getTableQueryTimeoutInfo());
        }

        // Test 11: Test exception handling when collectAllTable throws exception
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            
            // Use a local scope to ensure the mock is cleaned up after the test
            {
                new MockUp<AnalyzerUtils>() {
                    @Mock
                    public java.util.Map<com.starrocks.catalog.TableName, com.starrocks.catalog.Table> collectAllTable(
                            StatementBase statementBase) {
                        throw new RuntimeException("Mock exception for testing");
                    }
                };
                
                StmtExecutor executor = new StmtExecutor(testCtx, stmt);
                // Should fall back to session timeout when exception occurs
                int timeout = executor.getExecTimeout();
                int defaultTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
                Assertions.assertEquals(defaultTimeout, timeout);
                Assertions.assertNull(executor.getTableQueryTimeoutInfo());
            }
            // Mock is automatically cleaned up when it goes out of scope
        }

        // Test 12: Test that isSessionQueryTimeoutOverridden() exception propagates to getExecTimeout()
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            // Ensure ConnectContext.get() is not null for getExecTimeout()
            ConnectContext.set(testCtx);

            // Use a table without table_query_timeout so the result is deterministic (falls back to session timeout).
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx);

            // Mock isSessionQueryTimeoutOverridden() to throw, so getExecTimeout() should also throw
            new MockUp<ConnectContext>() {
                @Mock
                public boolean isSessionQueryTimeoutOverridden() {
                    throw new RuntimeException("Mock exception for isSessionQueryTimeoutOverridden()");
                }
            };

            try {
                StmtExecutor executor = new StmtExecutor(testCtx, stmt);
                // Since isSessionQueryTimeoutOverridden() throws, getExecTimeout() should also throw
                Assertions.assertThrows(RuntimeException.class, () -> executor.getExecTimeout());
            } finally {
                ConnectContext.remove();
            }
        }
    }
}
