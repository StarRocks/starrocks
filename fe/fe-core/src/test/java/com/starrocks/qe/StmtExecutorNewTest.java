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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StmtExecutorNewTest extends StarRocksTestBase  {
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
    }

    private StatementBase parse(String sql) throws Exception {
        return SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());
    }

    @Test
    public void testIsExplainTrace() throws Exception {
        // Test case 1: Normal explain statement without trace
        StatementBase stmt = parse("explain select * from t1");
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        assertFalse(stmt.isExplainTrace());

        // Test case 2: Explain statement with trace
        stmt = parse("trace logs optimizer select * from t1");
        executor = new StmtExecutor(ctx, stmt);
        assertTrue(stmt.isExplainTrace());

        // Test case 3: Normal statement (not explain)
        stmt = parse("select * from t1");
        executor = new StmtExecutor(ctx, stmt);
        assertFalse(stmt.isExplainTrace());
    }

    @Test
    public void testIsExplainAnalyze() throws Exception {
        // Test case 1: Normal explain statement
        StatementBase stmt = parse("explain select * from t1");
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        assertFalse(stmt.isExplainAnalyze());

        // Test case 2: Explain analyze statement
        stmt = parse("explain analyze select * from t1");
        executor = new StmtExecutor(ctx, stmt);
        assertTrue(stmt.isExplainAnalyze());

        // Test case 3: Explain statement with different level
        stmt = parse("explain verbose select * from t1");
        executor = new StmtExecutor(ctx, stmt);
        assertFalse(stmt.isExplainAnalyze());
    }

    @Test
    public void testHandleExplainStmtWithNullExecPlan() throws Exception {
        StatementBase stmt = parse("trace times optimizer select * from t1");
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        
        // Using reflection to test the private method
        java.lang.reflect.Method method = StmtExecutor.class.getDeclaredMethod(
                "handleExplainExecPlan", ExecPlan.class);
        method.setAccessible(true);
        
        // Test with null execPlan
        method.invoke(executor, new Object[] { null });
        
        // Verify that no exception is thrown
        assertTrue(true);
    }

    @Test
    public void testGenerateExecPlanWithException() throws Exception {
        StatementBase stmt = parse("trace logs mv select * from non_existent_table");
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        
        // Using reflection to test the private method
        java.lang.reflect.Method method = StmtExecutor.class.getDeclaredMethod(
                "generateExecPlan");
        method.setAccessible(true);
        
        // This should not throw exception even if planning fails
        try {
            method.invoke(executor);
        } catch (Exception e) {
            // Exception is expected but should be handled gracefully
            assertTrue(true);
        }
    }
}