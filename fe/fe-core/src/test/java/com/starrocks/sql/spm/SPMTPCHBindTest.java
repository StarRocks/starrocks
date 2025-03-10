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
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

public class SPMTPCHBindTest extends PlanTestBase {
    private static final Logger LOG = LogManager.getLogger(SPMTPCHBindTest.class);

    @BeforeAll
    public static void beforeAll() throws Exception {
        beforeClass();
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
                    SqlParser.parse(originStmt, connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            Preconditions.checkState(statementBase instanceof QueryStatement);
            return statementBase;
        } catch (Exception ex) {
            LOG.error(originStmt, ex);
            Assert.fail();
            throw ex;
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    public void validatePlanSQL(String name, String sql) {
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
    public void validateBindSQL(String name, String bindSQL, String planSQL) {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(bindSQL, planSQL);
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        String b = generator.getBindSql();
        String p = generator.getPlanStmtSQL();

        analyzeSuccess(b);
        analyzeSuccess(p);
    }

    @Test
    public void testConflict() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(TpchSQL.Q19, TpchSQL.Q19);
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        String p = generator.getPlanStmtSQL();
        assertContains(p, "L_SHIPMODE IN (_spm_const_list(9, 'AIR', 'AIR REG')");
    }

    @Test
    public void testConflict2() {
        String plan = "select sum(l_extendedprice* (1 - l_discount)) as revenue\n"
                + "from lineitem, part\n"
                + "where (p_partkey = l_partkey\n"
                + "            and p_brand = 'Brand#45'\n"
                + "            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                + "            and l_quantity >= 5 and l_quantity <= 5 + 10\n"
                + "            and p_size between 1 and 5\n"
                + "            and l_shipmode in ('AIR', 'AIR REG')\n"
                + "            and l_shipinstruct = 'DELIVER IN PERSON'\n"
                + "        ) or ( p_partkey = l_partkey\n"
                + "            and p_brand = 'Brand#11'\n"
                + "            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                + "            and l_quantity >= 15 and l_quantity <= 15 + 10\n"
                + "            and p_size between 1 and 10\n"
                + "            and l_shipmode in ('XXXX', 'XXXXXX')\n"
                + "            and l_shipinstruct = 'DELIVER IN PERSON');";
        String bind = "select sum(l_extendedprice* (1 - l_discount)) as revenue\n"
                + "from lineitem, part\n"
                + "where (p_partkey = l_partkey\n"
                + "            and p_brand = 'Brand#45'\n"
                + "            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                + "            and l_quantity >= 5 and l_quantity <= 5 + 10\n"
                + "            and p_size between 1 and 5\n"
                + "            and l_shipmode in ('AIR', 'AIR REG')\n"
                + "            and l_shipinstruct = 'DELIVER IN PERSON'\n"
                + "        ) or ( p_partkey = l_partkey\n"
                + "            and p_brand = 'Brand#11'\n"
                + "            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                + "            and l_quantity >= 15 and l_quantity <= 15 + 10\n"
                + "            and p_size between 1 and 10\n"
                + "            and l_shipmode in ('AIR', 'AIR REG')\n"
                + "            and l_shipinstruct = 'DELIVER IN PERSON');";
        {
            CreateBaselinePlanStmt stmt = createBaselinePlanStmt(bind, plan);
            SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
            Assertions.assertThrows(SemanticException.class, generator::execute);
        }

        {
            CreateBaselinePlanStmt stmt = createBaselinePlanStmt(plan, bind);
            SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
            Assertions.assertThrows(SemanticException.class, generator::execute);
        }
    }
    public static List<Arguments> testCases() {
        List<String> unsupported = Lists.newArrayList("q11", "q15", "q16", "q22");
        List<Arguments> list = Lists.newArrayList();
        TpchSQL.getAllSQL().forEach((k, v) -> {
            if (!unsupported.contains(k)) {
                list.add(Arguments.of("tpch." + k, v));
            }
        });
        return list;
    }

    public static List<Arguments> testCases2() {
        List<String> unsupported = Lists.newArrayList("q11", "q15", "q16", "q22");
        List<String> conflict = Lists.newArrayList();
        List<Arguments> list = Lists.newArrayList();
        TpchSQL.getAllSQL().forEach((k, v) -> {
            if (!unsupported.contains(k) && !conflict.contains(k)) {
                list.add(Arguments.of("tpch." + k, v, v));
            }
        });
        return list;
    }

}
