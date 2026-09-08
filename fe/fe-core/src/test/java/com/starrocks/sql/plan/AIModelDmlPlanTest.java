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

import com.starrocks.catalog.AIModel;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIModelSource;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TPlanNodeType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AIModelDmlPlanTest extends PlanTestBase {
    private static final String MODEL_NAME = "dml_plan_chat";

    @BeforeAll
    public static void createModelAndIcebergCatalog() throws Exception {
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        GlobalStateMgr.getCurrentState().getAIModelMgr().createModel(MODEL_NAME,
                Map.of("capability", "CHAT", "provider", "openai_compatible",
                        "endpoint", "https://models.example.test/v1/chat/completions",
                        "model", "dml-model", "credential_ref", "TEST_MODEL"), "", true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInsertSelectBindingsWithBothLockModes(boolean useDatabaseLock) {
        boolean original = connectContext.getSessionVariable().isCboUseDBLock();
        connectContext.getSessionVariable().setCboUseDBLock(useDatabaseLock);
        try {
            assertNamedModelPlan(plan("INSERT INTO tprimary (pk, v1, v2) "
                    + "SELECT pk1, ai_custom_query('" + MODEL_NAME + "', v3), v4 FROM tprimary1"));
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(original);
        }
    }

    @Test
    public void testUpdateAssignmentBindings() {
        assertNamedModelPlan(plan("UPDATE tprimary SET v1 = ai_custom_query('" + MODEL_NAME
                + "', v1) WHERE pk = 1"));
    }

    @Test
    public void testDeleteUncorrelatedSubqueryBindings() {
        assertNamedModelPlan(plan("DELETE FROM tprimary WHERE v1 IN "
                + "(SELECT ai_custom_query('" + MODEL_NAME + "', v3) FROM tprimary1)"));
    }

    @Test
    public void testMergeSourceSubqueryBindings() {
        assertNamedModelPlan(plan("MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t "
                + "USING (SELECT pk1 AS id, ai_custom_query('" + MODEL_NAME + "', v3) AS data FROM tprimary1) AS s "
                + "ON t.id = s.id WHEN MATCHED THEN UPDATE SET data = s.data"));
    }

    @Test
    public void testConditionalUpdateRemainsUnsupported() {
        assertRejected("UPDATE tprimary SET v1 = CASE WHEN pk = 1 THEN ai_custom_query('"
                + MODEL_NAME + "', v1) ELSE v1 END WHERE pk > 0", "conditional expression");
    }

    @Test
    public void testCorrelatedDeleteRemainsUnsupported() {
        assertRejected("DELETE FROM tprimary WHERE v1 IN (SELECT ai_custom_query('" + MODEL_NAME
                + "', v3) FROM tprimary1 WHERE pk1 = tprimary.pk)", "correlated AI function");
    }

    @Test
    public void testConditionalMergeAssignmentRemainsUnsupported() {
        assertRejected("MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t "
                + "USING (SELECT 1 AS id, 'prompt' AS data) AS s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET data = ai_custom_query('" + MODEL_NAME + "', s.data)",
                "conditional expression");
    }

    @Test
    public void testMergeJoinConditionRemainsUnsupported() {
        assertRejected("MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t "
                + "USING (SELECT 1 AS id, 'prompt' AS data) AS s "
                + "ON t.data = ai_custom_query('" + MODEL_NAME + "', s.data) "
                + "WHEN MATCHED THEN UPDATE SET data = s.data", "JOIN ON clause");
    }

    private static ExecPlan plan(String sql) {
        // EXPLAIN exercises the normal analyzer/authorizer/planner without leaving an unexecuted write transaction.
        String explain = "EXPLAIN " + sql;
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        connectContext.getDumpInfo().setOriginStmt(explain);
        StatementBase statement = SqlParser.parseSingleStatement(
                explain, connectContext.getSessionVariable().getSqlMode());
        return StatementPlanner.plan(statement, connectContext);
    }

    private static void assertNamedModelPlan(ExecPlan plan) {
        Assertions.assertNotNull(plan);
        AIModel snapshot = plan.getAIModelBindings().getRequiredModel(MODEL_NAME);
        Assertions.assertSame(GlobalStateMgr.getCurrentState().getAIModelMgr().getByName(MODEL_NAME), snapshot);
        String configurationId = plan.getAIModelBindings().configurationId(MODEL_NAME);
        List<TAIProjectNode> aiNodes = plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .map(node -> node.getAi_project_node()).toList();
        Assertions.assertFalse(aiNodes.isEmpty(), "The DML plan must physically execute its named-model call");
        for (TAIProjectNode node : aiNodes) {
            Assertions.assertEquals(Set.of(configurationId), node.getAi_model_configs().keySet());
            TAIModelConfiguration config = node.getAi_model_configs().get(configurationId);
            Assertions.assertEquals(TAIModelSource.AI_MODEL, config.getSource());
            Assertions.assertEquals(snapshot.getId(), config.getModel_id());
            Assertions.assertEquals("dml-model", config.getChat().getModel());
            Assertions.assertEquals("TEST_MODEL", config.getChat().getCredential_ref());
        }
    }

    private static void assertRejected(String sql, String context) {
        SemanticException error = Assertions.assertThrows(SemanticException.class, () -> plan(sql));
        Assertions.assertTrue(error.getMessage().contains(context), error.getMessage());
    }
}
