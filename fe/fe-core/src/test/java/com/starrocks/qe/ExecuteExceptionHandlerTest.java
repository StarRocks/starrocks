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
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TPlanNodeType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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
    public void testRpcRetryKeepsProviderSnapshotAfterAlter() throws Exception {
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        String name = "rpc_retry_provider";
        manager.createProvider(name, AIProviderType.CHAT, providerProperties("original"), "");
        try {
            String sql = "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7";
            StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan original = getExecPlan(sql);
            AIModelConfigs.ModelConfig snapshot = original.getAIProviderBindings().getRequiredConfig(name);
            ExecuteExceptionHandler.RetryContext retry =
                    new ExecuteExceptionHandler.RetryContext(0, original, connectContext, statement);

            manager.alterProvider(name, providerProperties("altered"), false);
            ExecuteExceptionHandler.handle(new RpcException("mock provider retry"), retry);

            Assertions.assertNotSame(original, retry.getExecPlan());
            Assertions.assertSame(original.getAIProviderBindings(), retry.getExecPlan().getAIProviderBindings());
            assertProviderSnapshot(retry.getExecPlan(), name, snapshot);
            Assertions.assertEquals("altered-model", getExecPlan(sql).getAIProviderBindings().getRequiredConfig(name).model());

            manager.alterProvider(name, Map.of("protocol", "anthropic"), false);
            retry.setRetryTime(1);
            ExecuteExceptionHandler.handle(new RpcException("mock protocol retry"), retry);
            Assertions.assertSame(original.getAIProviderBindings(), retry.getExecPlan().getAIProviderBindings());
            assertProviderSnapshot(retry.getExecPlan(), name, snapshot);
            Assertions.assertEquals("openai_compatible", snapshot.provider());
            SemanticException error = Assertions.assertThrows(SemanticException.class, () -> getExecPlan(sql));
            Assertions.assertEquals("AI functions require an OPENAI provider protocol, not ANTHROPIC", error.getDetailMsg());
        } finally {
            manager.dropProvider(name, true);
        }
    }

    @Test
    public void testSchemaRetryKeepsProviderSnapshotAfterRecreationAndDrop() throws Exception {
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        String name = "schema_retry_provider";
        manager.createProvider(name, AIProviderType.CHAT, providerProperties("original"), "");
        try {
            String sql = "select ai_custom_query('" + name + "', k1) from t7";
            StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan original = getExecPlan(sql);
            AIModelConfigs.ModelConfig snapshot = original.getAIProviderBindings().getRequiredConfig(name);
            ExecuteExceptionHandler.RetryContext retry =
                    new ExecuteExceptionHandler.RetryContext(0, original, connectContext, statement);

            manager.dropProvider(name, false);
            String recreatedId = manager.createProvider(name, AIProviderType.CHAT, providerProperties("recreated"), "");
            Assertions.assertNotEquals(snapshot.providerId(), recreatedId);
            ExecuteExceptionHandler.handle(new StarRocksException("invalid field name"), retry);

            Assertions.assertNotSame(original, retry.getExecPlan());
            Assertions.assertSame(original.getAIProviderBindings(), retry.getExecPlan().getAIProviderBindings());
            assertProviderSnapshot(retry.getExecPlan(), name, snapshot);
            Assertions.assertEquals(recreatedId, getExecPlan(sql).getAIProviderBindings().getRequiredConfig(name).providerId());

            manager.dropProvider(name, false);
            retry.setRetryTime(1);
            ExecuteExceptionHandler.handle(new StarRocksException("invalid field name"), retry);
            Assertions.assertSame(original.getAIProviderBindings(), retry.getExecPlan().getAIProviderBindings());
            assertProviderSnapshot(retry.getExecPlan(), name, snapshot);
        } finally {
            manager.dropProvider(name, true);
        }
    }

    private static Map<String, String> providerProperties(String version) {
        return Map.of("endpoint", "https://" + version + ".example.test/v1/chat/completions",
                "model", version + "-model", "api_key", version + "-test-key", "timeout_ms", "1200");
    }

    private static void assertProviderSnapshot(ExecPlan plan, String name, AIModelConfigs.ModelConfig expected) {
        Assertions.assertEquals(expected, plan.getAIProviderBindings().getRequiredConfig(name));
        String configId = plan.getAIProviderBindings().configurationId(name);
        List<TAIModelConfiguration> configurations = plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .map(node -> node.getAi_project_node().getAi_model_configs().get(configId))
                .toList();
        Assertions.assertFalse(configurations.isEmpty());
        configurations.forEach(configuration -> Assertions.assertEquals(expected.toThrift(), configuration));
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
