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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.planner.TpchSQL;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

public class SPMTPCHUseTest extends PlanTestBase {
    private static final Logger LOG = LogManager.getLogger(SPMTPCHUseTest.class);
    private static final List<String> UNSUPPORTED = Lists.newArrayList("q11", "q15", "q16", "q22");

    public static CreateBaselinePlanStmt createBaselinePlanStmt(String sql) {
        String createSql = "create baseline using " + sql;
        List<StatementBase> statements = SqlParser.parse(createSql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof CreateBaselinePlanStmt);
        return (CreateBaselinePlanStmt) statements.get(0);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        beforeClass();
        for (var entry : TpchSQL.getAllSQL().entrySet()) {
            if (UNSUPPORTED.contains(entry.getKey())) {
                continue;
            }
            CreateBaselinePlanStmt stmt = createBaselinePlanStmt(entry.getValue());
            SPMStmtExecutor.execute(connectContext, stmt);
        }
        connectContext.getSessionVariable().setEnableSPMRewrite(true);
    }

    @AfterAll
    public static void afterAll() {
        afterClass();
        connectContext.getGlobalStateMgr().getSqlPlanManager().dropAllBaselinePlans();
        connectContext.getSessionVariable().setEnableSPMRewrite(false);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    public void validate(String name, String sql) throws Exception {
        String s = getFragmentPlan(sql);
        if (UNSUPPORTED.contains(name)) {
            assertNotContains(s, "Using baseline plan");
        } else {
            assertContains(s, "Using baseline plan");
            assertNotContains(s, "spm_");
        }
    }

    public static List<Arguments> testCases() {
        List<Arguments> list = Lists.newArrayList();
        TpchSQL.getAllSQL().forEach((k, v) -> list.add(Arguments.of(k, v)));
        return list;
    }
}
