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

import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PrepareStmtPlannerTest extends PlanTestBase {
    private static final AtomicInteger NEXT_STMT_ID = new AtomicInteger();
    private static String oldEndpoint;
    private static String oldModel;
    private static String oldProvider;

    @BeforeAll
    public static void setUpAIConfiguration() throws Exception {
        oldEndpoint = Config.ai_default_chat_endpoint;
        oldModel = Config.ai_default_chat_model;
        oldProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "unit-test-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        Config.ai_default_chat_endpoint = oldEndpoint;
        Config.ai_default_chat_model = oldModel;
        Config.ai_default_chat_provider = oldProvider;
    }

    @Test
    public void testOrdinaryPointQueryStillUsesPreparedPlanCache() throws Exception {
        PreparedQuery prepared = prepare("select v1 from tprimary where pk = ?");

        ExecPlan first = execute(prepared, bigint(1));
        ExecPlan second = execute(prepared, bigint(2));

        Assertions.assertAll(
                () -> Assertions.assertTrue(prepared.context().isCached()),
                () -> Assertions.assertSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals(2, scanPredicateValue(second)));
    }

    @Test
    public void testAIPromptAndPointPredicateUseFreshPlans() throws Exception {
        assertAIPreparedStatementUsesFreshPlans("select ai_complete(?) from tprimary where pk = ?");
    }

    @Test
    public void testNestedAIPromptAndPointPredicateUseFreshPlans() throws Exception {
        assertAIPreparedStatementUsesFreshPlans(
                "select concat(ai_complete(?), '!') from tprimary where pk = ?");
    }

    private static void assertAIPreparedStatementUsesFreshPlans(String sql) throws Exception {
        PreparedQuery prepared = prepare(sql);

        ExecPlan first = execute(prepared, new StringLiteral("first prompt"), bigint(1));
        ExecPlan second = execute(prepared, new StringLiteral("second prompt"), bigint(2));

        Assertions.assertAll(
                () -> Assertions.assertFalse(prepared.context().isCached()),
                () -> Assertions.assertNotSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals("first prompt", aiPrompt(first)),
                () -> Assertions.assertEquals(1, scanPredicateValue(first)),
                () -> Assertions.assertEquals("second prompt", aiPrompt(second)),
                () -> Assertions.assertEquals(2, scanPredicateValue(second)));
    }

    private static PreparedQuery prepare(String query) throws Exception {
        String name = "prepared_" + NEXT_STMT_ID.incrementAndGet();
        boolean oldEnablePrepare = connectContext.getSessionVariable().isEnablePrepareStmt();
        connectContext.getSessionVariable().setEnablePrepareStmt(true);
        try {
            PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(
                    "prepare " + name + " from " + query, connectContext);
            PrepareStmtContext context = new PrepareStmtContext(stmt, connectContext, null);
            connectContext.putPreparedStmt(name, context);
            return new PreparedQuery(name, stmt, context);
        } finally {
            connectContext.getSessionVariable().setEnablePrepareStmt(oldEnablePrepare);
        }
    }

    private static ExecPlan execute(PreparedQuery prepared, Expr... values) {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setThreadLocalInfo();

        List<Expr> params = List.of(values);
        ExecuteStmt executeStmt = new ExecuteStmt(prepared.name(), params);
        Analyzer.analyze(executeStmt, connectContext);
        StatementBase assigned = prepared.statement().assignValues(params);
        return PrepareStmtPlanner.plan(executeStmt, assigned, connectContext);
    }

    private static IntLiteral bigint(long value) {
        return new IntLiteral(value, IntegerType.BIGINT);
    }

    private static long scanPredicateValue(ExecPlan plan) {
        PhysicalOlapScanOperator scan =
                findPhysicalOperator(plan.getPhysicalPlan(), PhysicalOlapScanOperator.class);
        ConstantOperator constant = scan.getPredicate().getChild(1).cast();
        return constant.getBigint();
    }

    private static String aiPrompt(ExecPlan plan) {
        PhysicalAIProjectOperator aiProject =
                findPhysicalOperator(plan.getPhysicalPlan(), PhysicalAIProjectOperator.class);
        CallOperator call = aiProject.getColumnRefMap().values().stream()
                .flatMap(ScalarOperator::asStream)
                .filter(CallOperator.class::isInstance)
                .map(CallOperator.class::cast)
                .filter(candidate -> candidate.getFnName().equalsIgnoreCase("ai_complete"))
                .findFirst()
                .orElseThrow();
        return call.getChild(0).asStream()
                .filter(ConstantOperator.class::isInstance)
                .map(ConstantOperator.class::cast)
                .findFirst()
                .orElseThrow()
                .getVarchar();
    }

    private static <T> T findPhysicalOperator(OptExpression root, Class<T> type) {
        if (type.isInstance(root.getOp())) {
            return type.cast(root.getOp());
        }
        for (OptExpression input : root.getInputs()) {
            T match = findPhysicalOperatorOrNull(input, type);
            if (match != null) {
                return match;
            }
        }
        throw new AssertionError("Missing physical operator " + type.getSimpleName());
    }

    private static <T> T findPhysicalOperatorOrNull(OptExpression root, Class<T> type) {
        if (type.isInstance(root.getOp())) {
            return type.cast(root.getOp());
        }
        for (OptExpression input : root.getInputs()) {
            T match = findPhysicalOperatorOrNull(input, type);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private record PreparedQuery(String name, PrepareStmt statement, PrepareStmtContext context) {
    }
}
