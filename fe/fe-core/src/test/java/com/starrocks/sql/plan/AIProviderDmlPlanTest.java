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

package com.starrocks.sql.plan;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AIProviderDmlPlanTest extends PlanTestBase {
    private static final String PROVIDER_NAME = "dml_plan_chat";

    @BeforeAll
    public static void createProviderAndIcebergCatalog() throws Exception {
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        GlobalStateMgr.getCurrentState().getAIProviderMgr().createProvider(PROVIDER_NAME, AIProviderType.CHAT,
                Map.of("endpoint", "https://models.example.test/v1/chat/completions",
                        "model", "dml-model", "api_key", "test-dml-key", "timeout_ms", "1800"), "");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInsertSelectBindingsWithBothLockModes(boolean useDatabaseLock) {
        boolean original = connectContext.getSessionVariable().isCboUseDBLock();
        connectContext.getSessionVariable().setCboUseDBLock(useDatabaseLock);
        try {
            assertProviderPlan(plan("INSERT INTO tprimary (pk, v1, v2) SELECT pk1, ai_complete(provider => '"
                    + PROVIDER_NAME + "', model => 'explicit-model', prompt => v3), v4 FROM tprimary1"));
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(original);
        }
    }

    @Test
    public void testUpdateAssignmentBindings() {
        assertProviderPlan(plan("UPDATE tprimary SET v1 = ai_complete(provider => '" + PROVIDER_NAME
                + "', model => 'explicit-model', prompt => v1) WHERE pk = 1"));
    }

    @Test
    public void testDeleteUncorrelatedSubqueryBindings() {
        assertProviderPlan(plan("DELETE FROM tprimary WHERE v1 IN (SELECT ai_complete(provider => '" + PROVIDER_NAME
                + "', model => 'explicit-model', prompt => v3) FROM tprimary1)"));
    }

    @Test
    public void testMergeSourceSubqueryBindings() {
        assertProviderPlan(plan("MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t USING (SELECT pk1 AS id, "
                + "ai_complete(provider => '" + PROVIDER_NAME + "', model => 'explicit-model', prompt => v3) AS "
                + "data FROM tprimary1) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET data = s.data"));
    }

    @Test
    public void testMergeSourceRequiresCallerFunctionAndProviderUsage() throws Exception {
        String user = "ai_merge_caller";
        executeAsRoot("CREATE USER '" + user + "' IDENTIFIED BY ''");
        ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
        caller.setCurrentRoleIds(Set.of());
        caller.setDatabase(connectContext.getDatabase());
        try {
            executeAsRoot("GRANT SELECT ON TABLE " + connectContext.getDatabase() + ".tprimary1 TO '" + user + "'");
            executeAsRoot("GRANT USAGE ON CATALOG iceberg0 TO '" + user + "'");
            String originalCatalog = connectContext.getCurrentCatalog();
            try {
                connectContext.setCurrentCatalog("iceberg0");
                executeAsRoot("GRANT SELECT, UPDATE ON TABLE unpartitioned_db.t0_v2 TO '" + user + "'");
            } finally {
                connectContext.setCurrentCatalog(originalCatalog);
            }
            String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t USING (SELECT pk1 AS id, "
                    + "ai_complete(provider => '" + PROVIDER_NAME
                    + "', model => 'explicit-model', prompt => v3) AS data FROM tprimary1) AS s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET data = s.data";
            ErrorReportException functionDenied = Assertions.assertThrows(ErrorReportException.class,
                    () -> plan(sql, caller));
            Assertions.assertTrue(functionDenied.getMessage().contains("AI FUNCTION"), functionDenied.getMessage());
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
            ErrorReportException providerDenied = Assertions.assertThrows(ErrorReportException.class,
                    () -> plan(sql, caller));
            Assertions.assertTrue(providerDenied.getMessage().contains("AI PROVIDER"), providerDenied.getMessage());
            executeAsRoot("GRANT USAGE ON AI PROVIDER " + PROVIDER_NAME + " TO '" + user + "'");
            assertProviderPlan(plan(sql, caller));
            executeAsRoot("REVOKE USAGE ON AI PROVIDER " + PROVIDER_NAME + " FROM '" + user + "'");
            Assertions.assertThrows(ErrorReportException.class, () -> plan(sql, caller));
        } finally {
            executeAsRoot("DROP USER '" + user + "'");
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testConditionalUpdateRemainsUnsupported() {
        assertRejected("UPDATE tprimary SET v1 = CASE WHEN pk = 1 THEN ai_complete(provider => '"
                + PROVIDER_NAME + "', model => 'explicit-model', prompt => v1) ELSE v1 END WHERE pk > 0",
                "conditional expression");
    }

    @Test
    public void testCorrelatedDeleteRemainsUnsupported() {
        assertRejected("DELETE FROM tprimary WHERE v1 IN (SELECT ai_complete(provider => '" + PROVIDER_NAME
                + "', model => 'explicit-model', prompt => v3) FROM tprimary1 WHERE pk1 = tprimary.pk)",
                "correlated AI function");
    }

    @Test
    public void testConditionalMergeAssignmentRemainsUnsupported() {
        assertRejected("MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t USING (SELECT 1 AS id, 'prompt' AS data) "
                + "AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET data = ai_complete(provider => '" + PROVIDER_NAME
                + "', model => 'explicit-model', prompt => s.data)",
                "conditional expression");
    }

    @Test
    public void testMergeJoinConditionRemainsUnsupported() {
        assertRejected("MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t USING (SELECT 1 AS id, 'prompt' AS data) "
                + "AS s ON t.data = ai_complete(provider => '" + PROVIDER_NAME + "', model => 'explicit-model', "
                + "prompt => s.data) WHEN MATCHED THEN UPDATE SET data = s.data", "JOIN ON clause");
    }

    private static ExecPlan plan(String sql) {
        return plan(sql, connectContext);
    }

    private static ExecPlan plan(String sql, ConnectContext context) {
        // EXPLAIN exercises the normal analyzer/authorizer/planner without leaving an unexecuted write transaction.
        String explain = "EXPLAIN " + sql;
        context.setQueryId(UUIDUtil.genUUID());
        context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setDumpInfo(new QueryDumpInfo(context));
        context.getDumpInfo().setOriginStmt(explain);
        StatementBase statement = SqlParser.parseSingleStatement(
                explain, context.getSessionVariable().getSqlMode());
        return StatementPlanner.plan(statement, context);
    }

    private static void executeAsRoot(String sql) throws Exception {
        try (var ignored = connectContext.bindScope()) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
        }
    }

    private static void assertProviderPlan(ExecPlan plan) {
        Assertions.assertNotNull(plan);
        List<TAIProjectNode> aiNodes = plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .map(node -> node.getAi_project_node()).toList();
        Assertions.assertFalse(aiNodes.isEmpty(), "The DML plan must physically execute its provider call");
        for (TAIProjectNode node : aiNodes) {
            Assertions.assertEquals(1, node.getAi_provider_mapping().size());
            TAIProvider config = node.getAi_provider_mapping().values().iterator().next();
            Assertions.assertEquals("dml-model", config.getParams().get("model"));
            Assertions.assertEquals("test-dml-key", config.getParams().get("api_key"));
            Assertions.assertEquals("1800", config.getParams().get("timeout_ms"));
            Assertions.assertEquals("https://models.example.test/v1/chat/completions", config.getParams().get("endpoint"));
            Assertions.assertEquals(TAIProviderProtocol.OPENAI, config.getProtocol());
        }
    }

    private static void assertRejected(String sql, String context) {
        SemanticException error = Assertions.assertThrows(SemanticException.class, () -> plan(sql));
        Assertions.assertTrue(error.getMessage().contains(context), error.getMessage());
    }
}
