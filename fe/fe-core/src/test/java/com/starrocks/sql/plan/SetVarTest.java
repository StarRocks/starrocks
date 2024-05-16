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

import com.google.common.collect.Maps;
import com.starrocks.analysis.HintNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SetVarTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        PlanTestBase.afterClass();
    }

    @Test
    public void testInsertStmt() throws Exception {
        SessionVariable variable = starRocksAssert.getCtx().getSessionVariable();
        int queryTimeout = variable.getQueryTimeoutS();

        // prepare table
        starRocksAssert.withTable("create table tbl (c1 int) properties('replication_num'='1')");
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                SessionVariable variables = execPlan.getConnectContext().getSessionVariable();
                Assert.assertEquals(10, variables.getQueryTimeoutS());
            }
        };

        new MockUp<DDLStmtExecutor>() {
            @Mock
            public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
                SessionVariable variables = context.getSessionVariable();
                Assert.assertFalse(variables.getEnableAdaptiveSinkDop());
                return null;
            }
        };

        // insert
        {
            String sql = "insert /*+set_var(query_timeout=10) */ into tbl values(1) ";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(queryTimeout, variable.getQueryTimeoutS());
        }

        // update
        {
            String sql = "update /*+set_var(query_timeout=10) */ tbl set c1 = 2 where c1 = 1";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(queryTimeout, variable.getQueryTimeoutS());
        }

        // delete
        {
            String sql = "delete /*+set_var(query_timeout=10) */ from tbl where c1 = 1";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(queryTimeout, variable.getQueryTimeoutS());
        }

        // load
        {
            boolean enableAdaptiveSinkDop = variable.getEnableAdaptiveSinkDop();
            String sql = "LOAD /*+set_var(enable_adaptive_sink_dop=false)*/ "
                    + "LABEL label0 (DATA INFILE('/path1/file') INTO TABLE tbl)";
            starRocksAssert.getCtx().executeSql(sql);
            Assert.assertEquals(enableAdaptiveSinkDop, variable.getEnableAdaptiveSinkDop());
        }

        starRocksAssert.dropTable("tbl");
    }

    @ParameterizedTest
    @MethodSource("genArguments")
    public void testMultiQueryBlocks(String query, Map<String, String> hints) throws Exception {
        starRocksAssert.withTable("create table if not exists tbl (c1 int) properties('replication_num'='1')");

        Assertions.assertEquals(hints, parseAndGetHints(query));
    }

    public static Stream<Arguments> genArguments() {
        Map<String, String> hints1 = Map.of("query_timeout", "10");
        Map<String, String> hints2 = Map.of("query_timeout", "1");
        Map<String, String> hints3 = Map.of("query_timeout", "10", "query_mem_limit", "1");
        Map<String, String> hints4 = Map.of("a", "1", "b", "abs(1)", "c", "(SELECT max(`c1`)\n" +
                "FROM `tbl`)");

        return Stream.of(
                // multi-block select
                Arguments.of("(select /*+set_var(query_timeout=1)*/avg(c1) from tbl) " +
                        "union all (select /*+set_var(query_timeout=10)*/sum(c1) from tbl) ", hints1),
                Arguments.of("(select /*+set_var(query_timeout=10)*/avg(c1) from tbl) " +
                        "union all (select /*+set_var(query_mem_limit=1)*/sum(c1) from tbl) ", hints3),
                Arguments.of("(select /*+set_var(query_timeout=10)*/ avg(c1) from tbl ) " +
                        "union (" +
                        "   select s1+1 from (" +
                        "       select /*+set_var(query_mem_limit=1)*/ sum(c1) as s1 from tbl " +
                        "   ) r1 " +
                        ") ", hints3),
                Arguments.of("insert /*+set_var(query_timeout=1) */ into tbl values(1) ", hints2),

                // insert select
                Arguments.of("insert into tbl select /*+set_var(query_timeout=1) */ * from tbl", hints2),
                Arguments.of("insert /*+set_var(query_timeout=1)*/ into tbl " +
                        "select /*+set_var(query_timeout=10) */ * from tbl", hints1),
                Arguments.of("insert /*+set_var(query_timeout=10)*/ into tbl " +
                        "select /*+set_var(query_mem_limit=1) */ * from tbl", hints3),
                Arguments.of("select /*+ SET_USER_VARIABLE(@a= 1, @ b = abs(1), " +
                        "@ c = (select max(c1) from tbl)) */ * from tbl", hints4)
        );
    }

    private static Map<String, String> parseAndGetHints(String sql) {
        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        Map<String, String> hints = Maps.newHashMap();
        if (stmts.get(0).isExistQueryScopeHint()) {
            for (HintNode hintNode : stmts.get(0).getAllQueryScopeHints()) {
                hints.putAll(hintNode.getValue());
            }
        }
        return hints;
    }
}
