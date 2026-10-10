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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AIPreparedInvokerViewPrivilegeTest extends PlanTestNoneDBBase {
    private static final String USER = "ai_prepared_invoker_user";
    private static final String DATABASE = "ai_prepared_invoker";
    private static String previousEndpoint;
    private static String previousModel;
    private static String previousProvider;

    @BeforeAll
    public static void setUpObjects() throws Exception {
        previousEndpoint = Config.ai_default_chat_endpoint;
        previousModel = Config.ai_default_chat_model;
        previousProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "rbac-model";
        Config.ai_default_chat_provider = "openai_compatible";
        starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
        starRocksAssert.withTable("CREATE TABLE prompts (id BIGINT, prompt VARCHAR(100)) "
                + "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withView("CREATE VIEW answers SECURITY INVOKER AS "
                + "SELECT id, ai_complete(prompt) AS answer FROM prompts");
    }

    @AfterAll
    public static void restoreConfiguration() {
        Config.ai_default_chat_endpoint = previousEndpoint;
        Config.ai_default_chat_model = previousModel;
        Config.ai_default_chat_provider = previousProvider;
    }

    @Test
    public void testPreparedInvokerChecksCurrentViewAndExecutedSnapshot() throws Exception {
        executeAsRoot("CREATE USER " + USER);
        ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(USER, "%"));
        caller.setCurrentRoleIds(Set.of());
        caller.setDatabase(DATABASE);
        caller.getSessionVariable().setEnablePrepareStmt(true);
        try {
            executeAsRoot("GRANT SELECT ON TABLE " + DATABASE + ".prompts TO " + USER);
            executeAsRoot("GRANT SELECT ON VIEW " + DATABASE + ".answers TO " + USER);
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO " + USER);
            PrepareStmt prepared;
            try (var ignored = caller.bindScope()) {
                prepared = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(
                        "PREPARE invoker_snapshot FROM SELECT answer FROM answers WHERE id = ?", caller);
                Authorizer.check(prepared, caller);
            }
            caller.putPreparedStmt(prepared.getName(), new PrepareStmtContext(prepared, caller, null));
            Assertions.assertEquals(Set.of("ai_complete"), families(executePrepared(caller, prepared)));

            executeAsRoot("ALTER VIEW answers AS SELECT id, ai_sentiment(prompt) AS answer FROM prompts");
            // INVOKER keeps main's live authorization of the current view definition.
            assertPreparedDenied(caller, prepared, "ai_sentiment");
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_sentiment TO " + USER);
            Assertions.assertEquals(Set.of("ai_sentiment"), families(planCurrentView(caller)));
            Assertions.assertEquals(Set.of("ai_complete"), families(executePrepared(caller, prepared)));

            // Permission to use the current view cannot authorize the old AI family still executed by PREPARE.
            executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM " + USER);
            Assertions.assertEquals(Set.of("ai_sentiment"), families(planCurrentView(caller)));
            assertPreparedDenied(caller, prepared, "ai_complete");
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO " + USER);
            Assertions.assertEquals(Set.of("ai_complete"), families(executePrepared(caller, prepared)));

            executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_sentiment FROM " + USER);
            assertPreparedDenied(caller, prepared, "ai_sentiment");
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_sentiment TO " + USER);
            Assertions.assertEquals(Set.of("ai_complete"), families(executePrepared(caller, prepared)));
        } finally {
            caller.removePreparedStmt("invoker_snapshot");
            executeAsRoot("DROP USER " + USER);
            connectContext.setThreadLocalInfo();
        }
    }

    private static void assertPreparedDenied(ConnectContext caller, PrepareStmt prepared, String family) {
        ErrorReportException error = Assertions.assertThrows(ErrorReportException.class,
                () -> executePrepared(caller, prepared));
        Assertions.assertTrue(error.getMessage().contains(family), error.getMessage());
    }

    private static ExecPlan executePrepared(ConnectContext caller, PrepareStmt prepared) {
        try (var ignored = caller.bindScope()) {
            caller.setQueryId(UUIDUtil.genUUID());
            caller.setExecutionId(UUIDUtil.toTUniqueId(caller.getQueryId()));
            ExecuteStmt execute = new ExecuteStmt(prepared.getName(), List.of(new IntLiteral(1, IntegerType.BIGINT)));
            Analyzer.analyze(execute, caller);
            return PrepareStmtPlanner.plan(execute, prepared.assignValues(execute.getParamsExpr()), caller);
        }
    }

    private static ExecPlan planCurrentView(ConnectContext caller) throws Exception {
        try (var ignored = caller.bindScope()) {
            caller.setQueryId(UUIDUtil.genUUID());
            caller.setExecutionId(UUIDUtil.toTUniqueId(caller.getQueryId()));
            return StatementPlanner.plan(UtFrameUtils.parseStmtWithNewParser("SELECT answer FROM answers", caller), caller);
        }
    }

    private static Set<String> families(ExecPlan plan) {
        return plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .flatMap(node -> node.getAi_project_node().getSlot_map().values().stream())
                .flatMap(expression -> expression.getNodes().stream())
                .filter(node -> node.isSetFn() && node.getFn().getBinary_type() == TFunctionBinaryType.AI)
                .map(node -> node.getFn().getName().getFunction_name()).collect(Collectors.toSet());
    }

    private static void executeAsRoot(String sql) throws Exception {
        try (var ignored = connectContext.bindScope()) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
        }
    }
}
