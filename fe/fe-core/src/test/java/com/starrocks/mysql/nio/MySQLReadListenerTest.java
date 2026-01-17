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

package com.starrocks.mysql.nio;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

public class MySQLReadListenerTest {
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private ConnectProcessor connectProcessor;
    @Mocked
    private StmtExecutor stmtExecutor;

    private MySQLReadListener listener;

    private boolean invokeIsTerminated() throws Exception {
        Method method = MySQLReadListener.class.getDeclaredMethod("isTerminated");
        method.setAccessible(true);
        return (boolean) method.invoke(listener);
    }

    @BeforeEach
    public void setUp() {
        // Reset GracefulExitFlag before each test
        listener = new MySQLReadListener(ctx, connectProcessor);
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testIsTerminatedWhenTerminatedFlagIsTrue() throws Exception {
        // Set terminated flag to true
        Deencapsulation.setField(listener, "terminated", true);

        boolean result = invokeIsTerminated();

        Assertions.assertTrue(result, "isTerminated should return true when terminated flag is true");
    }

    @Test
    public void testIsTerminatedWhenGracefulExitAndNotPreQuerySQL() throws Exception {
        // Set terminated flag to false
        Deencapsulation.setField(listener, "terminated", false);

        // Mark graceful exit
        GracefulExitFlag.markGracefulExit();

        // Parse a non-pre-query SQL statement (regular SELECT)
        StatementBase stmt = SqlParser.parseSingleStatement("select sleep(10)", SqlModeHelper.MODE_DEFAULT);

        new Expectations() {
            {
                connectProcessor.getExecutor();
                result = stmtExecutor;
                stmtExecutor.getParsedStmt();
                result = stmt;
            }
        };

        boolean result = invokeIsTerminated();

        Assertions.assertTrue(result,
                "isTerminated should return true when graceful exit is active and statement is not pre-query SQL");
    }

    @Test
    public void testIsTerminatedWhenGracefulExitAndPreQuerySQL() throws Exception {
        // Set terminated flag to false
        Deencapsulation.setField(listener, "terminated", false);

        // Mark graceful exit
        GracefulExitFlag.markGracefulExit();

        // Parse a pre-query SQL statement (select @@query_timeout)
        StatementBase stmt = SqlParser.parseSingleStatement("select @@query_timeout", SqlModeHelper.MODE_DEFAULT);

        new Expectations() {
            {
                connectProcessor.getExecutor();
                result = stmtExecutor;

                stmtExecutor.getParsedStmt();
                result = stmt;
            }
        };

        boolean result = invokeIsTerminated();

        Assertions.assertFalse(result,
                "isTerminated should return false when graceful exit is active but statement is pre-query SQL");
    }

    @Test
    public void testIsTerminatedWithNullParsedStmt() throws Exception {
        // Set terminated flag to false
        Deencapsulation.setField(listener, "terminated", false);

        // Mark graceful exit
        GracefulExitFlag.markGracefulExit();

        new Expectations() {
            {
                connectProcessor.getExecutor();
                result = stmtExecutor;

                stmtExecutor.getParsedStmt();
                result = null;
            }
        };

        boolean result = invokeIsTerminated();

        // When parsedStmt is null, isPreQuerySQL returns false, so isTerminated should return true
        Assertions.assertTrue(result,
                "isTerminated should return true when graceful exit is active and parsedStmt is null");
    }

    @Test
    public void testIsTerminatedWithNullExecutor() throws Exception {
        // Set terminated flag to false
        Deencapsulation.setField(listener, "terminated", false);

        // Mark graceful exit
        GracefulExitFlag.markGracefulExit();

        new Expectations() {
            {
                connectProcessor.getExecutor();
                result = null;
            }
        };

        boolean result = invokeIsTerminated();

        // When executor is null, we should return false (not terminated yet)
        Assertions.assertFalse(result,
                "isTerminated should return false when executor is null");
    }

    @Test
    public void testIsTerminatedWithSetStatement() throws Exception {
        // Set terminated flag to false
        Deencapsulation.setField(listener, "terminated", false);

        // Mark graceful exit
        GracefulExitFlag.markGracefulExit();

        // Parse a pre-query SET statement
        StatementBase stmt = SqlParser.parseSingleStatement("set query_timeout=600", SqlModeHelper.MODE_DEFAULT);

        new Expectations() {
            {
                connectProcessor.getExecutor();
                result = stmtExecutor;

                stmtExecutor.getParsedStmt();
                result = stmt;
            }
        };

        boolean result = invokeIsTerminated();

        Assertions.assertFalse(result,
                "isTerminated should return false when graceful exit is active but statement is a pre-query SET");
    }

    @Test
    public void testIsTerminatedWithConnectionIdFunction() throws Exception {
        // Set terminated flag to false
        Deencapsulation.setField(listener, "terminated", false);

        // Mark graceful exit
        GracefulExitFlag.markGracefulExit();

        // Parse connection_id() function call
        StatementBase stmt = SqlParser.parseSingleStatement("select connection_id()", SqlModeHelper.MODE_DEFAULT);

        new Expectations() {
            {
                connectProcessor.getExecutor();
                result = stmtExecutor;

                stmtExecutor.getParsedStmt();
                result = stmt;
            }
        };

        boolean result = invokeIsTerminated();

        Assertions.assertFalse(result,
                "isTerminated should return false when graceful exit is active but statement is connection_id()");
    }
}
