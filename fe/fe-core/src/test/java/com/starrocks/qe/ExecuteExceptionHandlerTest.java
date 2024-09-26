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

import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.UserException;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.rpc.RpcException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class ExecuteExceptionHandlerTest extends PlanTestBase {

    @Test
    public void testHandleRemoteFileNotFoundException_1() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assert.assertThrows(RemoteFileNotFoundException.class,
                () -> ExecuteExceptionHandler.handle(new RemoteFileNotFoundException("mock"), retryContext));
    }


    @Test
    public void testHandleRemoteFileNotFoundException_2() throws Exception {
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        String sql = "select * from hive0.tpch.customer_view";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        try {
            ExecuteExceptionHandler.handle(new RemoteFileNotFoundException("mock"), retryContext);
        } catch (Exception e) {
            fail("should not throw any exception");
        }
    }

    @Test
    public void testHandleRpcException() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        ExceptionChecker.expectThrowsNoException(() ->
                ExecuteExceptionHandler.handle(new RpcException("mock"), retryContext));
        // execPlan is built
        Assert.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testHandleUseException_1() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        try {
            ExecuteExceptionHandler.handle(new UserException("invalid field name"), retryContext);
            Assert.assertTrue(retryContext.getExecPlan() != execPlan);
        } catch (Exception e) {
            fail("should not throw any exception");
        }
    }

    @Test
    public void testHandleUseException_2() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assert.assertThrows(UserException.class,
                () -> ExecuteExceptionHandler.handle(new UserException("other exception"), retryContext));
    }

    @Test
    public void testHandleUseException_3() throws Exception {
        // cancel with backend not alive, should retry
        String sql = "select * from t1";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assert.assertEquals(retryContext.getExecPlan(), execPlan);

        ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(new UserException(
                InternalErrorCode.CANCEL_NODE_NOT_ALIVE_ERR, FeConstants.BACKEND_NODE_NOT_FOUND_ERROR), retryContext));

        Assert.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }
}
