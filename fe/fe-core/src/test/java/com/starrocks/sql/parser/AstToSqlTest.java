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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public class AstToSqlTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest
    @MethodSource("testSqls")
    void testAstToSQl(String sql) {
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, connectContext);
        String afterSql = AstToSQLBuilder.toSQL(stmt);
        try {
            SqlParser.parse(afterSql, connectContext.getSessionVariable());
        } catch (Exception e) {
            fail("failed to parse the sql: " + afterSql + ". errMsg: " + e.getMessage());
        }
    }


    private static Stream<Arguments> testSqls() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("with with_t_0 as (select t0.days_add(t0.v1, -1692245077) from t0) select * from t1, with_t_0;");
        sqls.add("with with_t_0 as (select t0.days_add(v1, -1692245077) as c from t0) select * from t1, with_t_0;");
        sqls.add("with with_t_0 (abc) as (select t0.days_add(v1, -1692245077) as c from t0) select * from t1, with_t_0;");
        sqls.add("with with_t_0 as (select t0.days_add(v1, -1692245077) as c from t0) " +
                "select * from t1, with_t_0 where t1.abs(v4) = 1 order by t1.abs(v6) = 2;");
        return sqls.stream().map(e -> Arguments.of(e));
    }
}
