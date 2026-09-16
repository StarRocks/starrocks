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
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.lake.LakeMetaVersionNotFoundException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.rpc.RpcException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TStatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.fail;

public class ExecuteExceptionHandlerTest extends PlanTestBase {

    @Test
    public void testHandleRemoteFileNotFoundException_1() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertThrows(RemoteFileNotFoundException.class,
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
        Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testHandleLakeMetaVersionNotFoundException() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertEquals(execPlan, retryContext.getExecPlan());

        // Retrying the existing fragments would reuse scan ranges pointing at the unreadable version,
        // so the handler has to plan the statement again instead.
        ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(
                new LakeMetaVersionNotFoundException("lake tablet metadata version not found, tablet_id=10001, "
                        + "partition_id=10002, version=144847: Not found"), retryContext));

        Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testLakeMetaVersionNotFoundIsRetryable() {
        Assertions.assertTrue(
                ExecuteExceptionHandler.isRetryableStatus(TStatusCode.LAKE_META_VERSION_NOT_FOUND));
        // A generic NOT_FOUND stays non-retryable: only the dedicated status says the scan version,
        // not the tablet, is the problem.
        Assertions.assertFalse(ExecuteExceptionHandler.isRetryableStatus(TStatusCode.NOT_FOUND));
        Assertions.assertFalse(ExecuteExceptionHandler.isRetryableStatus(TStatusCode.CANCELLED));
    }

    /**
     * The retry log reports the version the fresh plan picked for the partition BE could not read,
     * which is the batch's final version once published. This pins that lookup: it must search the
     * plan's OlapScanNodes by physical partition id, and degrade to empty -- logged as "unknown" --
     * rather than throw when the fresh plan no longer scans that partition.
     *
     * <p>The map is seeded here rather than taken from planning: scan ranges are assigned during
     * scheduling, so a plain getExecPlan() leaves scanPartitionVersions empty. The key space itself
     * is guaranteed inside OlapScanNode.addScanRangeLocations(), which puts physicalPartition.getId()
     * into both this map and TInternalScanRange.partition_id -- the id BE echoes back to us.
     */
    @Test
    public void testFindScanVersionLooksUpByPhysicalPartitionId() throws Exception {
        ExecPlan execPlan = getExecPlan("select * from t0");
        OlapScanNode scanNode = null;
        for (ScanNode node : execPlan.getScanNodes()) {
            if (node instanceof OlapScanNode) {
                scanNode = (OlapScanNode) node;
                break;
            }
        }
        Assertions.assertNotNull(scanNode);
        scanNode.getScanPartitionVersions().put(10002L, 144849L);

        Assertions.assertEquals(OptionalLong.of(144849L), ExecuteExceptionHandler.findScanVersion(execPlan, 10002L));
        Assertions.assertEquals(OptionalLong.empty(), ExecuteExceptionHandler.findScanVersion(execPlan, -1L));
        Assertions.assertEquals(OptionalLong.empty(), ExecuteExceptionHandler.findScanVersion(null, 10002L));
    }

    @Test
    public void testHandleUseException_1() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        try {
            ExecuteExceptionHandler.handle(new StarRocksException("invalid field name"), retryContext);
            Assertions.assertTrue(retryContext.getExecPlan() != execPlan);
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
        Assertions.assertThrows(StarRocksException.class,
                () -> ExecuteExceptionHandler.handle(new StarRocksException("other exception"), retryContext));
    }

    @Test
    public void testHandleUseException_3() throws Exception {
        // cancel with backend not alive, should retry
        String sql = "select * from t1";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertEquals(retryContext.getExecPlan(), execPlan);

        ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(new StarRocksException(
                InternalErrorCode.CANCEL_NODE_NOT_ALIVE_ERR, FeConstants.BACKEND_NODE_NOT_FOUND_ERROR), retryContext));

        Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }
}
