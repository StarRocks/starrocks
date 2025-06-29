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

import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SelectUsingAliasTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testAliasInSelect() {
        // test when from is null
        String sql = "select 'x' as a, 1 as b, null, cast(b as bigInt) as d";
        testSqlRewrite(sql, "SELECT 'x' AS a, 1 AS b, NULL AS NULL, CAST(1 AS BIGINT) AS d");

        // test when from is not null
        sql = "select 'x' as a, 1 as b, null as c, cast(b as smallInt) as d " +
                "from test_all_type";
        testSqlRewrite(sql, "SELECT 'x' AS a, 1 AS b, NULL AS c, CAST(1 AS SMALLINT) AS d\n" +
                "FROM test.test_all_type");

        // test alias in where
        sql = "select 'x' as a, 1 as b, null as c, cast(b as smallInt) as d " +
                "from test_all_type where a is not null";
        String aliasInWhereSql = sql;
        Assert.assertThrows("Column 'a' cannot be resolved", SemanticException.class,
                () -> getFragmentPlan(aliasInWhereSql));

        // test alias in function
        sql = "select t1a as t1a, trim(t1a) as a, a as b, concat(a,',',b) as e, b as d from test_all_type";
        testSqlRewrite(sql, "SELECT t1a, trim(t1a) AS a, trim(t1a) AS b, " +
                "concat(trim(t1a), ',', trim(t1a)) AS e, trim(t1a) AS d\n" +
                "FROM test.test_all_type");

        // test alias defined later than using
        sql = "select concat(a,','),trim(t1a) as a from test_all_type";
        testSqlRewrite(sql, "SELECT concat(trim(t1a), ',') AS concat(trim(t1a), ','), trim(t1a) AS a\n" +
                "FROM test.test_all_type");

        // test alias in group by
        sql = "select trim(t1a) as a,count(*) as c from test_all_type group by a";
        testSqlRewrite(sql, "SELECT trim(t1a) AS a, count(*) AS c\n" +
                "FROM test.test_all_type\n" +
                "GROUP BY a");

        // test alias in group by and order by
        sql = "select trim(t1a) as a,count(*) as c from test_all_type group by a order by c";
        testSqlRewrite(sql, "SELECT trim(t1a) AS a, count(*) AS c\n" +
                "FROM test.test_all_type\n" +
                "GROUP BY a ORDER BY c ASC ");

        // test alias in agg and in having
        sql = "select t1c as b, sum(b) as s from test_all_type group by b having s>1 order by s";
        testSqlRewrite(sql, "SELECT t1c AS b, sum(t1c) AS s\n" +
                "FROM test.test_all_type\n" +
                "GROUP BY b\n" +
                "HAVING (sum(t1c)) > 1 ORDER BY s ASC ");

        // test alias cascading referring
        sql = "select concat(a,',',b) as e,concat(t1a,cast(t1c as string)) as a, a as b,b as d from test_all_type";
        testSqlRewrite(sql, "SELECT concat(concat(t1a, CAST(t1c AS VARCHAR(65533))), ','" +
                ", concat(t1a, CAST(t1c AS VARCHAR(65533)))) AS e, " +
                "concat(t1a, CAST(t1c AS VARCHAR(65533))) AS a, " +
                "concat(t1a, CAST(t1c AS VARCHAR(65533))) AS b, " +
                "concat(t1a, CAST(t1c AS VARCHAR(65533))) AS d\n" +
                "FROM test.test_all_type");

        // test cyclic aliases
        sql = "select concat(trim(d),',') as c,trim(c) as d from (" +
                "select trim(t1a) as a from test_all_type group by a" +
                ") b";
        String cyclicAliasSql = sql;
        Assert.assertThrows("Cyclic aliases", SemanticException.class, () -> getFragmentPlan(cyclicAliasSql));
    }

    @Test
    public void testAliasSameAsColumn() {
        boolean oldValue = connectContext.getSessionVariable().getEnableGroupbyUseOutputAlias();

        // test alias is same as column name
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(false);
        String sql = "select trim(t1a) as t1c, concat(t1c,','),t1c from test_all_type";
        testSqlRewrite(sql, "SELECT trim(t1a) AS t1c, concat(t1c, ',') AS concat(t1c, ','), t1c\n" +
                "FROM test.test_all_type");

        // test alias is same as column name
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(true);
        sql = "select trim(t1a) as t1c, concat(t1c,','),t1c from test_all_type";
        testSqlRewrite(sql, "SELECT trim(t1a) AS t1c, " +
                "concat(trim(t1a), ',') AS concat(trim(test.test_all_type.t1a), ','), " +
                "trim(t1a) AS trim(test.test_all_type.t1a)\n" +
                "FROM test.test_all_type");

        // test test_all_type.t1c should be column name
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(true);
        sql = "select trim(t1a) as t1c, concat(t1c,','),test_all_type.t1c from test_all_type";
        testSqlRewrite(sql, "SELECT trim(test.test_all_type.t1a) AS t1c, " +
                "concat(trim(test.test_all_type.t1a), ',') AS concat(trim(test.test_all_type.t1a), ','), " +
                "test.test_all_type.t1c\n" +
                "FROM test.test_all_type", false);

        // test t1a in trim(t1a) should be column name
        sql = "select trim(t1a) as t1a from test_all_type";
        testSqlRewrite(sql, "SELECT trim(test.test_all_type.t1a) AS t1a\n" +
                "FROM test.test_all_type", false);

        // test t1a in concat(t1a,',') should be alias
        sql = "select trim(t1a),concat(t1a,',') as t1a from test_all_type";
        testSqlRewrite(sql, "SELECT trim(concat(test.test_all_type.t1a, ',')) AS trim(concat(t1a, ',')), " +
                "concat(test.test_all_type.t1a, ',') AS t1a\n" +
                "FROM test.test_all_type", false);

        // test t1a in concat(t1a,',') should be alias
        sql = "select concat(t1a,',') as t1a,trim(t1a) from test_all_type";
        testSqlRewrite(sql, "SELECT concat(test.test_all_type.t1a, ',') AS t1a, " +
                "trim(concat(test.test_all_type.t1a, ',')) AS trim(concat(test.test_all_type.t1a, ','))\n" +
                "FROM test.test_all_type", false);

        // test t1a in concat(t1c, ',', t1a) should be alias
        // test t1c in concat(t1c, ',', t1a) should be column name
        sql = "select trim(t1a),concat(t1c, ',', t1a) as t1c, concat(t1a,',') as t1a from test_all_type";
        testSqlRewrite(sql, "SELECT trim(concat(test.test_all_type.t1a, ',')) AS trim(concat(t1a, ',')), " +
                "concat(test.test_all_type.t1c, ',', concat(test.test_all_type.t1a, ',')) AS t1c, " +
                "concat(test.test_all_type.t1a, ',') AS t1a\n" +
                "FROM test.test_all_type", false);

        // test t1a in concat(t1a, ',') should be column name
        // test t1a in concat(t1a, ',', t1a) should be alias
        // test t1c in concat(t1a, ',', t1a) should be column name
        sql = "select concat(t1a, ',', t1c) as t1c, concat(t1a,',') as t1a from test_all_type";
        testSqlRewrite(sql, "SELECT concat(concat(test.test_all_type.t1a, ','), ',', test.test_all_type.t1c) AS t1c, " +
                "concat(test.test_all_type.t1a, ',') AS t1a\n" +
                "FROM test.test_all_type", false);

        // test alias is same as column name in group by
        sql = "select t1a, trim(t1a) as t1c,count(*) from test_all_type group by t1a, t1c";
        testSqlRewrite(sql, "SELECT test.test_all_type.t1a, trim(test.test_all_type.t1a) AS t1c, count(*) AS count(*)\n" +
                "FROM test.test_all_type\n" +
                "GROUP BY t1a, t1c", false);
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(oldValue);
    }

    @Test
    public void testHasStar() {
        String sql = "select t1a, * from test_all_type";
        testSqlRewrite(sql, "SELECT t1a, t1a, t1b, t1c, t1d, t1e, t1f, t1g, id_datetime, id_date, id_decimal\n" +
                "FROM test.test_all_type");

        sql = "select t1a as a, * from test_all_type";
        testSqlRewrite(sql, "SELECT t1a AS a, t1a, t1b, t1c, t1d, t1e, t1f, t1g, id_datetime, id_date" +
                ", id_decimal\n" +
                "FROM test.test_all_type");

        sql = "select *, rand(), current_date() from test_all_type";
        testSqlRewrite(sql, "SELECT t1a, t1b, t1c, t1d, t1e, t1f, t1g, id_datetime, id_date, id_decimal, " +
                "rand() AS rand(), current_date() AS current_date()\n" +
                "FROM test.test_all_type");
    }

    @Test
    public void testCaseInsensitive() {
        // test when from is null
        String sql = "select 'x' as a, 1 as B, null, cast(b as bigInt) as d";
        testSqlRewrite(sql, "SELECT 'x' AS a, 1 AS B, NULL AS NULL, CAST(1 AS BIGINT) AS d");

        // test cyclic aliases
        sql = "select concat(trim(d),',') as C,trim(c) as d from (" +
                "select trim(t1a) as a from test_all_type group by a" +
                ") b";
        String cyclicAliasSql = sql;
        Assert.assertThrows("Cyclic aliases", SemanticException.class, () -> getFragmentPlan(cyclicAliasSql));

        // test alias is same as column name and in group by
        sql = "select t1a, trim(t1a) as t1C,count(*) from test_all_type group by t1a, t1c";
        testSqlRewrite(sql, "SELECT test.test_all_type.t1a, trim(test.test_all_type.t1a) AS t1C, count(*) AS count(*)\n" +
                "FROM test.test_all_type\n" +
                "GROUP BY test.test_all_type.t1a, test.test_all_type.t1c", false);
    }

    @Test
    public void testAmbiguous() {
        // test duplicate alias but not ambiguous
        String sql = "select v1 as v, v2 as v from t0";
        testSqlRewrite(sql, "SELECT v1 AS v, v2 AS v\nFROM test.t0");

        // test duplicate alias and ambiguous in order by
        Assert.assertThrows("column 'v' is ambiguous", SemanticException.class,
                () -> getFragmentPlan("select v1 as v, v2 as v from t0 order by v"));

        // test duplicate alias and ambiguous in group by
        Assert.assertThrows("column 'v' is ambiguous", SemanticException.class,
                () -> getFragmentPlan("select v1 as v, v2 as v from t0 group by v"));

        // test duplicate alias and ambiguous in function
        Assert.assertThrows("column 'v' is ambiguous", SemanticException.class,
                () -> getFragmentPlan("select v1 as v, v2 as v, concat(trim(v),',') from t0"));

        // test duplicate alias and ambiguous in function
        Assert.assertThrows("column 'v' is ambiguous", SemanticException.class,
                () -> getFragmentPlan("select trim(v1) as v, trim(v2) as v, concat(trim(v),',') from t0"));

        // test normal alias not ambiguous
        sql = "select trim(v1) as v, concat(v,',') from t0";
        testSqlRewrite(sql, "SELECT trim(v1) AS v, concat(trim(v1), ',') AS concat(trim(test.t0.v1), ',')\n" +
                "FROM test.t0");
    }

    @Test
    public void testAliasSameAsColumnName() throws Exception {
        // test duplicate alias is same as column name
        String sql = "select v1 v1, v2 + 1 v1, abs(v1) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 2: v2 + 1\n" +
                "  |  <slot 5> : abs(1: v1)");

        sql = "select v1 v1, v1 v1, abs(v1) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : abs(1: v1)");

        Exception exception = Assert.assertThrows(SemanticException.class, () -> {
            // test duplicate alias is different from column name
            String duplicateAlias = "select v1 v1, v2 v1, abs(v1) from t0 order by v1";
            getFragmentPlan(duplicateAlias);
        });
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Column 'v1' is ambiguous."));

        exception = Assert.assertThrows(SemanticException.class, () -> {
            // test duplicate alias is different from column name
            String duplicateAlias = "select v1 v4, v2 + 1 v4, abs(v4) from t0";
            getFragmentPlan(duplicateAlias);
        });
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Column v4 is ambiguous"));
    }

    private void testSqlRewrite(String sql, String expected) {
        testSqlRewrite(sql, expected, true);
    }

    private void testSqlRewrite(String sql, String expected, boolean withoutTbl) {
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, connectContext);
        String afterSql = new AstToSQLBuilder.AST2SQLBuilderVisitor(false, withoutTbl, true).visit(stmt);
        Assert.assertEquals(expected, afterSql.replaceAll("`", ""));
    }
}
