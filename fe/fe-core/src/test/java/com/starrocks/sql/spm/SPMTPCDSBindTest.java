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
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

public class SPMTPCDSBindTest extends TPCDSPlanTestBase {
    private static final Logger LOG = LogManager.getLogger(SPMTPCDSBindTest.class);

    private static final List<String> UNSUPPORTED = List.of();

    @BeforeAll
    public static void beforeAll() throws Exception {
        beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        SPMFunctions.enableSPMParamsPrint = true;
    }

    public CreateBaselinePlanStmt createBaselinePlanStmt(String sql) {
        String createSql = "create baseline using " + sql;
        List<StatementBase> statements = SqlParser.parse(createSql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof CreateBaselinePlanStmt);
        return (CreateBaselinePlanStmt) statements.get(0);
    }

    public CreateBaselinePlanStmt createBaselinePlanStmt(String bind, String sql) {
        bind = bind.replace(";", "");
        sql = sql.replace(";", "");
        String createSql = "create baseline on " + bind + " using " + sql;
        List<StatementBase> statements = SqlParser.parse(createSql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof CreateBaselinePlanStmt);
        return (CreateBaselinePlanStmt) statements.get(0);
    }

    public static StatementBase analyzeSuccess(String originStmt) {
        try {
            StatementBase statementBase =
                    com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            Preconditions.checkState(statementBase instanceof QueryStatement);
            return statementBase;
        } catch (Exception ex) {
            LOG.error(originStmt, ex);
            Assertions.fail();
            throw ex;
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    public void validatePlan(String name, String sql) {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(sql);
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        String bindSQL = generator.getBindSql();
        String planSQL = generator.getPlanStmtSQL();

        analyzeSuccess(bindSQL);
        analyzeSuccess(planSQL);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases2")
    public void validatePlan2(String name, String bind, String plan) {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(bind, plan);
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        String bindSQL = generator.getBindSql();
        String planSQL = generator.getPlanStmtSQL();

        analyzeSuccess(bindSQL);
        analyzeSuccess(planSQL);
    }

    @Test
    public void validateCase() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(Q69);
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        String bindSQL = generator.getBindSql();
        String planSQL = generator.getPlanStmtSQL();

        logSysInfo(bindSQL);
        logSysInfo("\n\n====================================\n\n");
        logSysInfo(planSQL);
    }

    public static List<Arguments> testCases() {
        List<Arguments> list = Lists.newArrayList();
        getSqlMap().forEach((k, v) -> {
            if (!UNSUPPORTED.contains(k)) {
                list.add(Arguments.of(k, v));
            }
        });
        return list;
    }

    public static List<Arguments> testCases2() {
        List<Arguments> list = Lists.newArrayList();
        List<String> conflict = Lists.newArrayList("query06", "query23-1", "query23-2", "query44");
        getSqlMap().forEach((k, v) -> {
            if (!UNSUPPORTED.contains(k) && !conflict.contains(k)) {
                list.add(Arguments.of(k, v, v));
            }
        });
        return list;
    }

    @Test
    public void validate3() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(Q06);
        SPMStmtExecutor.execute(connectContext, stmt);
    }
}
