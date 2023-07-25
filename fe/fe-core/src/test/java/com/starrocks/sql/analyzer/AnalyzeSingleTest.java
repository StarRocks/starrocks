// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.common.Config;
import com.starrocks.common.util.LogUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeWithoutTestView;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;

public class AnalyzeSingleTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSingle() {
        analyzeSuccess("select v1 from t0");
        analyzeSuccess("select v1 as k from t0");
        analyzeSuccess("select v1 from t0 where v2 = 1");
        analyzeSuccess("select v1, v2, v3 from t0 where v2 = 1");
        analyzeSuccess("select v1, v2, v3 from t0 where v2 = 1 and v1 = 2");
        analyzeSuccess("select * from t0 t where t.v1 = 1");
        analyzeFail("select * from t0 t where t0.v1 = 1");

        // Test lower and upper

        analyzeSuccess("select V1, v2 from t0 where V3 = 1");
        analyzeFail("select * from T0");

        // Test ambiguous reference
        analyzeSuccess("select v1, v1 from t0");
        analyzeWithoutTestView("select * from (select v1, v1 from t0) a");
        analyzeWithoutTestView("select * from (select v1 as v, v2 as v from t0) a");

        // Test invalid reference
        analyzeFail("select error from t0");
        analyzeFail("select v1 from t_error");

        analyzeSuccess("select v1 from t0 temporary partition(t1,t2)");
        analyzeFail("SELECT v1,v2,v3 FROM t0 INTO OUTFILE \"hdfs://path/to/result_\""
                + "FORMAT AS PARQUET PROPERTIES" +
                "(\"broker.name\" = \"my_broker\"," +
                "\"broker.hadoop.security.authentication\" = \"kerberos\"," +
                "\"line_delimiter\" = \"\n\", \"max_file_size\" = \"100MB\");", "Only support CSV format");

        analyzeSuccess("SELECT v1,v2,v3 FROM t0  INTO OUTFILE \"hdfs://path/to/result_\""
                + "FORMAT AS CSV PROPERTIES" +
                "(\"broker.name\" = \"my_broker\"," +
                "\"broker.hadoop.security.authentication\" = \"kerberos\"," +
                "\"line_delimiter\" = \"\n\", \"max_file_size\" = \"100MB\");");

        analyzeSuccess("select v1 as location from t0");
    }

    @Test
    public void testPrefix() {
        analyzeSuccess("select t0.v1 from t0");
        analyzeSuccess("select t0.*, v3, v2 from t0");
        analyzeSuccess("select v1 from test.t0");
        analyzeSuccess("select test.t0.v1 from test.t0");
        analyzeSuccess("select test.t0.v1 from t0");
        analyzeSuccess("select k from (select test.t0.v1 as k from test.t0) a");
        analyzeSuccess("select v1 from (select v1 from test.t0) a");

        // Test prefix in group by
        analyzeSuccess("select v1 from t0 group by t0.v1");
        analyzeSuccess("select t0.v1 from t0 group by v1");

        analyzeFail("select t2.v1 from t0");
        analyzeFail("select v1 from test_error.t0");
    }

    @Test
    public void testStar() {
        analyzeSuccess("select * from t0");
        analyzeSuccess("select *, v1 from t0");
        analyzeSuccess("select v1, * from t0");
        analyzeSuccess("select * from (select * from t0) a");
        analyzeSuccess("select v1 from (select * from t0) a");
        analyzeSuccess("select v1, v2, v3 from (select * from t0) a");
        analyzeSuccess("select a.v1 from (select * from t0) a");
        analyzeSuccess("select * from t0 where v2 = 1 and v1 = 2");

        analyzeFail("select error from (select * from t0) a");
        analyzeFail("select t0.v1 from (select * from t0) a");

        analyzeFail("select t.* from t0");
        analyzeFail("select t0.* from t0 t");

        analyzeFail("select * from t0 t GROUP BY t.v1, t.v2, t.v3", "combine '*' in select list with GROUP BY: *");
        analyzeFail("select * from tall t GROUP BY t.ta, t.tb, t.tc", "combine '*' in select list with GROUP BY: *");
    }

    @Test
    public void testFilter() {
        analyzeSuccess("select v1 from t0 where v1 = 1");
        analyzeSuccess("select v1 from t0 where v2 = 1 and v3 = 5");
        analyzeSuccess("select v1 from t0 where v1 = v2");
        analyzeSuccess("select v1 from t0 where v2");

        analyzeFail("select v1 from t0 where sum(v2) > 1");
        analyzeFail("select v1 from t0 where error = 5");
        analyzeFail("select v1 from t0 where error = v1");
    }

    @Test
    public void testCTE() {
        // Test CTE column name resolve
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0) select * from testWith");
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0) select a from testWith");
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0) select testWith.a, b from testWith");
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0) select a, b, c from testWith");
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0) select a + 1, b + c from testWith");
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0) " +
                "select * from (select a + 1 as k1, b + c as k2 from testWith) temp");
        analyzeFail("with testWith (a, b, c) as (select v1, v2, v3 from t0) select v1, v2 from testWith");

        // Test CTE name resolve
        analyzeSuccess("WITH t9 AS (SELECT * FROM t0), t0 AS (SELECT 2 FROM t9) SELECT * FROM t9");
        analyzeFail("with ta as (select * from tb), tb as (select 2 from t0) select * from ta");
        analyzeSuccess("with t0 as (select * from t1), t1 as (select * from t0) select * from t1");
        analyzeSuccess("with te as (select * from t0) select * from te as t");

        // Test anonymous cte
        analyzeSuccess("with cte1 as (select * from t0) select * from cte1");
        analyzeSuccess("with cte1 as (select * from t0), cte2 as (select * from t0) select * from cte2");
        analyzeSuccess("with cte1 as (select * from t0) select v1, v2 from cte1");

        // Test constant cte
        analyzeSuccess("with t as (select 1 from t0) select * from t");
        analyzeSuccess("with t0 as (select 1 from t0) select * from t0");
        analyzeSuccess("with t0 (a, b) as (select 1, 2 from t0) select a, b from t0");

        // Test Set operation
        analyzeSuccess("with testWith (a, b, c) as " +
                "(select v1, v2, v3 from t0 union select v4,v5,v6 from t1) select * from testWith");
        analyzeSuccess("with testWith (a, b, c) as " +
                "(select v1, v2, v3 from t0 except select v4,v5,v6 from t1) select * from testWith");
        analyzeSuccess("with testWith (a, b, c) as (select v1, v2, v3 from t0 intersect " +
                "select v4,v5,v6 from t1) select * from testWith");

        // Test cte used in set
        analyzeSuccess("with w as (select * from t0) select * from w union all select * from w");
        analyzeSuccess("with w as (select * from t0) select * from w except select * from w");
        analyzeSuccess("with w as (select * from t0) select * from w intersect select * from w");
        analyzeSuccess(" with w as (select * from t0) select 1 from w");

        // Test cte with different relationId
        analyzeSuccess("with w as (select * from t0) select v1,sum(v2) from w group by v1 " +
                "having v1 in (select v3 from w where v2 = 2)");
    }

    @Test
    public void testAggregate() {
        analyzeSuccess("select sum(v1) from t0");
        analyzeSuccess("select sum(v1), sum(v2) from t0");
        analyzeSuccess("select sum(v2) from t0 group by v2");
        analyzeSuccess("select v1, v2, sum(v3) from t0 group by v1, v2");
        analyzeSuccess("select v1+1, sum(v2) from t0 group by v1+1");
        analyzeSuccess("select v1+1, sum(v2) from t0 group by v1");
        analyzeSuccess("select v1+1, v1, sum(v2) from t0 group by v1");
        analyzeSuccess("select v1,v3,max(v3) from t0 group by v1,v3");
        analyzeSuccess("select v1,v3,max(v3),sum(v2) from t0 group by v1,v3");
        analyzeFail("select v1, sum(v2) from t0");
        analyzeFail("select v1, v2, sum(v3) from t0 group by v2");
        analyzeFail("select v3, sum(v2) from t0 group by v1, v2");
        analyzeFail("select v1, sum(v2) from t0 group by v1+1");
        analyzeFail("select * from t0 where sum(v1) > 1");
        analyzeFail("select * from t0 group by sum(v1)");
        analyzeFail("select sum(v1) / v2 FROM t0");

        // Group by expression
        analyzeSuccess("select v1+1, sum(v2) from t0 group by v1+1");
        analyzeSuccess("SELECT - v1 AS v1 FROM t0 GROUP BY v1,v2,v3 HAVING NOT + v2 BETWEEN NULL AND v1");
        analyzeFail("select v1, sum(v2) from t0 group by v1+1");

        // Group by ordinal
        analyzeSuccess("select v1, v2, sum(v3) from t0 group by 1,2");
        analyzeSuccess("select v1, 1, sum(v2) from t0 group by v1, 1");
        analyzeSuccess("select v1, 1, sum(v2) from t0 group by 1, 2");
        analyzeSuccess("select v1, v2 + 2, sum(v3) from t0 group by v1, 2");
        analyzeSuccess("select v1 as k1, v2 + 2 as k2, sum(v3) from t0 group by 1,2");
        analyzeFail("select * from t0 group by 1");
        analyzeFail("select v1, v2, sum(v3) from t0 group by 1");
        analyzeFail("select v1, v2, sum(v3) from t0 group by 1,2,3");
    }

    @Test
    public void testHaving() {
        analyzeSuccess("select sum(v1) from t0 having sum(v1) > 0");
        analyzeSuccess("select sum(v1) from t0 having sum(v2) > 0");
        analyzeSuccess("select v2,sum(v1) from t0 group by v2 having v2 > 0");
        analyzeSuccess("select sum(v1) from t0 having avg(v1) - avg(v2) > 10");
        analyzeSuccess("select sum(v1) from t0 where v2 > 2 having sum(v1) > 0");
        analyzeSuccess("select v1+1 from t0 group by v1+1 having v1 + 1 = 1");
        analyzeSuccess("select v1+1 from t0 group by v1 having v1 + 1 = 1");

        analyzeFail("select sum(v1) from t0 having v2");
        analyzeFail("select sum(v1) from t0 having v2 > 0");
        analyzeFail("select sum(v1) from t0 having v1 > 0");
        analyzeFail("select v2, sum(v1) from t0 group by v2 having v1 > 0");
        analyzeFail("select sum(v1) from t0 having sum(v1)");
    }

    @Test
    public void testSort() {
        // Simple test
        analyzeSuccess("select v1 from t0 order by v1");
        analyzeSuccess("select v1 from t0 order by v2");
        analyzeSuccess("select v1 from t0 order by v1 asc ,v2 desc");
        analyzeSuccess("select v1 from t0 order by v1 limit 10");
        analyzeSuccess("select v1 from t0 order by v1, v2");
        analyzeSuccess("select v1 from t0 limit 2, 10");

        // Test output scope resolve
        analyzeSuccess("select v1 as v from t0 order by v+1");
        analyzeSuccess("select v1+1 as v from t0 order by v");
        analyzeSuccess("select v1+2 as v,* from t0 order by v+1");
        analyzeSuccess("select v1, sum(v2) as v from t0 group by v1 order by v");
        analyzeSuccess("select v1, sum(v2) as v from t0 group by v1 order by sum(v2)");
        analyzeSuccess("select v1+1 as v from t0 group by v1+1 order by v");
        analyzeSuccess("select v1+1 as v from t0 group by v order by v");

        // Test order by with aggregation
        analyzeSuccess("select v1, sum(v2) from t0 group by v1 order by v1");
        analyzeSuccess("select v1, sum(v2) from t0 group by v1 order by sum(v2)");
        analyzeSuccess("select v1, sum(v2) from t0 group by v1 order by max(v3)");
        analyzeSuccess("select v1, sum(v2) from t0 group by v1 order by v1, max(v3)");
        analyzeSuccess("select v2, sum(v1) as s from t0 group by v2 order by s");
        analyzeSuccess("select v1,sum(v2) from t0 group by v1 order by max(v3)");
        analyzeSuccess("select v1,v3,sum(v2) from t0 group by v1,v3 order by max(v2)");
        analyzeFail("select v1, sum(v2) from t0 group by v1 order by v2");
        analyzeFail("select v1, sum(v2) from t0 group by v1 order by v3");
        analyzeFail("select sum(v1) from t0 order by v2+1");
        analyzeFail("select v1,sum(v2) from t0 group by v1 order by v1,max(v3),v2");
        analyzeFail("select v1, sum(v2) from t0 group by v1 order by max(error_field)");

        // Test ambiguous reference
        analyzeSuccess("select v1, v1 from t0 order by v1");
        analyzeFail("select v1 as v, v2 as v from t0 order by v");
        analyzeSuccess("select v1 as v, v1 as v from t0 order by v");
    }

    @Test
    public void testExpression() {
        // Test ArithmeticExpr
        analyzeSuccess("select v1 + 1 as k from t0");
        analyzeSuccess("select v1 + v2 as k from t0");

        // Test InPredicate
        analyzeSuccess("select * from t0 where v1 in (1, 2)");

        // Test LIKE
        analyzeSuccess("select * from tall where ta like \"%a%\"");
        analyzeFail("select * from t0 where v1 kike \"starrocks%\"");

        // Test function
        analyzeSuccess("select round(v1) from t0");
        analyzeFail("select error_function_name(v1) from t0");
        //        analyzeSuccess("select count(distinct v1,v2) from t0");
        analyzeSuccess("select BITMAP_UNION(case when 1=1 then b1 else NULL end) from test_object");
        analyzeSuccess("select BITMAP_UNION(case when 1=1 then b1 else b2 end) from test_object");
        analyzeFail("select BITMAP_UNION(case when 1=1 then b1 else h1 end) from test_object");

        analyzeFail("select max(TIMEDIFF(NULL, NULL)) from t0");
        analyzeSuccess("select abs(TIMEDIFF(NULL, NULL)) from t0");
        analyzeFail(" SELECT t0.v1 FROM t0 GROUP BY t0.v1 HAVING ((MAX(TIMEDIFF(NULL, NULL))) IS NULL)");

        analyzeSuccess("select right('foo', 1)");
        analyzeSuccess("select left('foo', 1)");

        /*
         * For support mysql embedded quotation`
         * In a double-quoted string, two double-quotes are combined into one double-quote
         */
        QueryStatement statement = (QueryStatement) analyzeSuccess("select '\"\"' ");
        Assert.assertEquals("'\"\"'", AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));
        statement = (QueryStatement) analyzeSuccess("select \"\"\"\" ");
        Assert.assertEquals("'\"'", AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));
        statement = (QueryStatement) analyzeSuccess("select \"7\\\"\\\"\"");
        Assert.assertEquals("'7\"\"'", AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));
        statement = (QueryStatement) analyzeSuccess("select '7'''");
        Assert.assertEquals("'7\\''", AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));
        statement = (QueryStatement) analyzeSuccess("SELECT '7\\'\\''");
        Assert.assertEquals("'7\\'\\''", AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));
        statement = (QueryStatement) analyzeSuccess("select \"Hello ' World ' !\"");
        Assert.assertEquals("'Hello \\' World \\' !'",
                AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));
        statement = (QueryStatement) analyzeSuccess("select 'Hello \" World \" !'");
        Assert.assertEquals("'Hello \" World \" !'",
                AstToStringBuilder.toString(statement.getQueryRelation().getOutputExpression().get(0)));

        analyzeSuccess("select @@`sql_mode`");
    }

    @Test
    public void testCast() {
        analyzeSuccess("select cast(v1 as varchar) from t0 group by cast(v1 as varchar)");
        analyzeSuccess("select cast(v1 as varchar) + 1 from t0 group by cast(v1 as varchar)");
    }

    @Test
    public void testColumnNames() {
        QueryRelation query = ((QueryStatement) analyzeSuccess("" +
                "select v1, v2, v3," +
                "cast(v1 as int), cast(v1 as char), cast(v1 as varchar), cast(v1 as decimal(10,5)), cast(v1 as boolean)," +
                "abs(v1)," +
                "v1 * v1 / v1 % v1 + v1 - v1 DIV v1," +
                "v2&~v1|v3^1,v1+20, case v2 when v3 then 1 else 0 end " +
                "from t0")).getQueryRelation();

        Assert.assertEquals(
                "v1,v2,v3," +
                        "CAST(v1 AS INT),CAST(v1 AS CHAR),CAST(v1 AS VARCHAR),CAST(v1 AS DECIMAL64(10,5)),CAST(v1 AS BOOLEAN)," +
                        "abs(v1)," +
                        "((((v1 * v1) / v1) % v1) + v1) - (v1 DIV v1)," +
                        "(v2 & (~v1)) | (v3 ^ 1)," +
                        "v1 + 20," +
                        "CASE v2 WHEN v3 THEN 1 ELSE 0 END",
                String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select * from (select v1 as v, sum(v2) from t0 group by v1) a " +
                "inner join (select v1 as v,v2 from t0 order by v3) b on a.v = b.v")
        ).getQueryRelation();
        Assert.assertEquals("v,sum(v2),v,v2", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select * from (select v1 as v, sum(v2) from t0 group by v1) a " +
                "inner join (select v1 as v,v2 from t0 order by v3) b on a.v = b.v"))
                .getQueryRelation();
        Assert.assertEquals("v,sum(v2),v,v2", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select * from (select v1 as tt from t0,t1) a " +
                "inner join (select v1 as v,v2 from t0 order by v3) b on a.tt = b.v"))
                .getQueryRelation();
        Assert.assertEquals("tt,v,v2", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select *, v1+1 from t0")).getQueryRelation();
        Assert.assertEquals("v1,v2,v3,v1 + 1", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select t1.* from t0 left outer join t1 on t0.v1+3=t1.v4"))
                .getQueryRelation();
        Assert.assertEquals("v4,v5,v6", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select v1+1,a.* from (select * from t0) a")).getQueryRelation();
        Assert.assertEquals("v1 + 1,v1,v2,v3", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess(
                "select v2+1,a.* from (select v1 as v, v2, v3+2 from t0) a left join t1 on a.v = t1.v4"))
                .getQueryRelation();
        Assert.assertEquals("v2 + 1,v,v2,v3 + 2", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select 1 as a, 2 as b")).getQueryRelation();
        Assert.assertEquals("a,b", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select * from (values (1,2,3), (4,5,6)) v;")).getQueryRelation();
        Assert.assertEquals("column_0,column_1,column_2", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess(
                "select * from (select t0.*, v4 from t0 inner join t1 on v1 = v5) tmp")).getQueryRelation();
        Assert.assertEquals("v1,v2,v3,v4", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select t1.* from t0 inner join t1 on v1 = v4 order by v1"))
                .getQueryRelation();
        Assert.assertEquals("v4,v5,v6", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select v4,v1,t1.* from t0 inner join t1 on v1 = v4 order by v1"))
                .getQueryRelation();
        Assert.assertEquals("v4,v1,v4,v5,v6", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select v1+2 as v, * from t0 order by v+1")).getQueryRelation();
        Assert.assertEquals("v,v1,v2,v3", String.join(",", query.getColumnOutputNames()));
    }

    @Test
    public void testDual() {
        analyzeSuccess("select 1,2,3 from dual");
        analyzeFail("select * from dual", "No tables used");
    }

    @Test
    public void testLogicalBinaryPredicate() {
        QueryStatement queryStatement = (QueryStatement) analyzeSuccess("select * from test.t0 where v1 = 1 && v2 = 2");
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertTrue(selectRelation.getPredicate() instanceof CompoundPredicate);
        Assert.assertEquals(((CompoundPredicate) selectRelation.getPredicate()).getOp(), CompoundPredicate.Operator.AND);

        queryStatement = (QueryStatement) analyzeSuccess("select * from test.t0 where v1 = 1 || v2 = 2");
        selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        Assert.assertTrue(selectRelation.getPredicate() instanceof CompoundPredicate);
        Assert.assertEquals(((CompoundPredicate) selectRelation.getPredicate()).getOp(), CompoundPredicate.Operator.OR);
    }

    @Test
    public void testSqlMode() {
        ConnectContext connectContext = getConnectContext();
        analyzeSuccess("select 'a' || 'b' from t0");

        StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse("select true || false from t0",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals("SELECT TRUE OR FALSE FROM test.t0",
                AstToStringBuilder.toString(statementBase));

        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT);
        statementBase = com.starrocks.sql.parser.SqlParser.parse("select 'a' || 'b' from t0",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals("SELECT concat('a', 'b') FROM test.t0",
                AstToStringBuilder.toString(statementBase));

        statementBase = SqlParser.parse("select * from  tall where ta like concat(\"h\", \"a\", \"i\")||'%'",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals(
                "SELECT * FROM test.tall WHERE test.tall.ta LIKE (concat(concat('h', 'a', 'i'), '%'))",
                AstToStringBuilder.toString(statementBase));

        connectContext.getSessionVariable().setSqlMode(0);
        statementBase = SqlParser.parse("select * from  tall where ta like concat(\"h\", \"a\", \"i\")|| true",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals(
                "SELECT * FROM test.tall WHERE (test.tall.ta LIKE (concat('h', 'a', 'i'))) OR TRUE",
                AstToStringBuilder.toString(statementBase));

        analyzeSuccess("select * from  tall where ta like concat(\"h\", \"a\", \"i\")||'%'");

        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_SORT_NULLS_LAST);
        statementBase = SqlParser.parse("select * from  tall order by ta",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals("SELECT * FROM test.tall ORDER BY test.tall.ta ASC NULLS LAST ",
                AstToStringBuilder.toString(statementBase));

        statementBase = SqlParser.parse("select * from  test.tall order by test.tall.ta desc",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals(
                "SELECT * FROM test.tall ORDER BY test.tall.ta DESC NULLS FIRST ",
                AstToStringBuilder.toString(statementBase));

        connectContext.getSessionVariable().setSqlMode(0);
        statementBase = SqlParser.parse("select * from  test.tall order by test.tall.ta",
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statementBase, connectContext);
        Assert.assertEquals(
                "SELECT * FROM test.tall ORDER BY test.tall.ta ASC ",
                AstToStringBuilder.toString(statementBase));
    }

    @Test
    public void testSqlSplit() {
        List<StatementBase> list = SqlParser.parse("select * from t1;", 0);
        Assert.assertEquals(1, list.size());

        list = SqlParser.parse("select * from t1", 0);
        Assert.assertEquals(1, list.size());

        list = SqlParser.parse("select * from t1;select * from t2;", 0);
        Assert.assertEquals(2, list.size());

        list = SqlParser.parse("select * from t1 where a1 = 'x\"x;asf';", 0);
        Assert.assertEquals(1, list.size());

        list = SqlParser.parse("-- xxx;\nselect 1;", 0);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.get(0) instanceof QueryStatement);

        list = SqlParser.parse("/* xx; x */select 1;", 0);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.get(0) instanceof QueryStatement);

        list = SqlParser.parse("select array_contains([], cast('2021-01--1 08:00:00' as datetime)) \n from t0", 0);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.get(0) instanceof QueryStatement);

        list = SqlParser.parse("select array_contains([], cast('2021-01--1 08:00:00' as datetime)) --x\n from t0", 0);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.get(0) instanceof QueryStatement);

        list = SqlParser.parse("select array_contains([], cast('2021-01--1 08:00:00' as datetime)) --x;x\n from t0", 0);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.get(0) instanceof QueryStatement);
    }

    @Test
    public void testTablet() {
        StatementBase statementBase = analyzeSuccess("SELECT v1 FROM t0  TABLET(1,2,3) LIMIT 200000");
        SelectRelation queryRelation = (SelectRelation) ((QueryStatement) statementBase).getQueryRelation();
        Assert.assertEquals("[1, 2, 3]", ((TableRelation) queryRelation.getRelation()).getTabletIds().toString());
    }

    @Test
    public void testSetVar() {
        StatementBase statementBase = analyzeSuccess("SELECT /*+ SET_VAR(time_zone='Asia/Shanghai') */ " +
                "current_timestamp() AS time");
        SelectRelation selectRelation = (SelectRelation) ((QueryStatement) statementBase).getQueryRelation();
        Assert.assertEquals("Asia/Shanghai", selectRelation.getSelectList().getOptHints().get("time_zone"));

        statementBase = analyzeSuccess("select /*+ SET_VAR(broadcast_row_limit=1) */ * from t0");
        selectRelation = (SelectRelation) ((QueryStatement) statementBase).getQueryRelation();
        Assert.assertEquals("1", selectRelation.getSelectList().getOptHints().get("broadcast_row_limit"));
    }

    @Test
    public void testLowCard() {
        String sql = "select * from test.t0 [_META_]";
        QueryStatement queryStatement = (QueryStatement) analyzeSuccess(sql);
        Assert.assertTrue(((TableRelation) ((SelectRelation) queryStatement.getQueryRelation()).getRelation()).isMetaQuery());
    }

    @Test
    public void testSync() {
        analyzeSuccess("sync");
    }

    @Test
    public void testUnsupportedStatement() {
        analyzeSuccess("start transaction");
        analyzeSuccess("start transaction with consistent snapshot");
        analyzeSuccess("begin");
        analyzeSuccess("begin work");
        analyzeSuccess("commit");
        analyzeSuccess("commit work");
        analyzeSuccess("commit and no chain release");
        analyzeSuccess("rollback");
    }

    @Test
    public void testASTChildCountLimit() {
        Config.expr_children_limit = 5;
        analyzeSuccess("select * from test.t0 where v1 in (1,2,3,4,5)");
        analyzeSuccess("select * from test.t0 where v1 in (1,2,3,4)");

        analyzeFail("select * from test.t0 where v1 in (1,2,3,4,5,6)",
                "Expression child number 6 exceeded the maximum 5");
        analyzeFail("select [1,2,3,4,5,6]",
                "Expression child number 6 exceeded the maximum 5");
        analyzeFail("select array<int>[1,2,3,4,5,6]",
                "Expression child number 6 exceeded the maximum 5");
        analyzeFail("select * from (values(1,2,3,4,5,6)) t",
                "Expression child number 6 exceeded the maximum 5");
        analyzeFail("insert into t0 values(1,2,3),(1,2,3),(1,2,3),(1,2,3),(1,2,3),(1,2,3)",
                "Expression child number 6 exceeded the maximum 5");
        analyzeFail("insert into t0 values(1,2,3,4,5,6)",
                "Expression child number 6 exceeded the maximum 5");

        Config.expr_children_limit = 100000;
        analyzeSuccess("select * from test.t0 where v1 in (1,2,3,4,5,6)");
    }

    @Test
    public void testOrderByWithSameColumnName() {
        analyzeFail("select * from t0, tnotnull order by v1", "Column 'v1' is ambiguous");
        analyzeSuccess("select * from t0, tnotnull order by t0.v1");

        analyzeFail("select t0.v1 from t0, tnotnull order by v2", "Column 'v2' is ambiguous");
        analyzeSuccess("select t0.v1 from t0, tnotnull order by v1");
        analyzeSuccess("select tnotnull.v1 from t0, tnotnull order by v1");
        analyzeSuccess("select t0.v1 from t0, tnotnull order by t0.v1");
        analyzeFail("select t0.v1 as v from t0, tnotnull order by v1", "Column 'v1' is ambiguous");
        analyzeSuccess("select t0.v1 as v from t0, tnotnull order by t0.v1");
        analyzeFail("select t0.v1, tnotnull.v1 from t0, tnotnull order by v1", "Column 'v1' is ambiguous");
    }

    @Test
    public void testOutputNamesWithDB() {
        QueryRelation query = ((QueryStatement) analyzeSuccess(
                "select t0.v1, v1 from t0"))
                .getQueryRelation();
        Assert.assertEquals("v1,v1", String.join(",", query.getColumnOutputNames()));
        analyzeFail("create view v as select t0.v1, v1 from t0", "Duplicate column name 'v1'");

        query = ((QueryStatement) analyzeSuccess(
                "select * from t0, t1"))
                .getQueryRelation();
        Assert.assertEquals("v1,v2,v3,v4,v5,v6", String.join(",", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess(
                "select t0.*, abs(t0.v1), abs(v1) from t0, t1"))
                .getQueryRelation();
        Assert.assertEquals("v1,v2,v3,abs(t0.v1),abs(v1)", String.join(",", query.getColumnOutputNames()));

        analyzeSuccess("select v1 as v from t0 order by v1");
        analyzeSuccess("select v1 as v from t0 order by t0.v1");
        analyzeFail("select v1 as v from t0 order by test.v",
                "Column '`test`.`v`' cannot be resolved");

        analyzeFail("create view v as select * from t0,tnotnull", "Duplicate column name 'v1'");
    }

    @Test
    public void testRemoveCommentAndLineSeparator1() {
        String sql = "#comment\nselect /* comment */ /*+SET_VAR(disable_join_reorder=true)*/* from    " +
                "tbl where-- comment\n" +
                "col = 1 #comment\r\n" +
                "\tand /*\n" +
                "comment\n" +
                "comment\n" +
                "*/ col = \"con   tent\n" +
                "contend\" and col = \"''```中\t文  \\\"\r\n\\r\\n\\t\\\"英  文\" and `col`= 'abc\"bcd\\\'';";
        String res = LogUtil.removeCommentAndLineSeparator(sql);
        Assert.assertEquals("select /*+SET_VAR(disable_join_reorder=true)*/* from tbl where col = 1 " +
                "and col = \"con   tent\n" +
                "contend\" and col = \"''```中\t文  \\\"\r\n\\r\\n\\t\\\"英  文\" and `col`= 'abc\"bcd\\'';", res);
    }

    @Test
    public void testRemoveCommentAndLineSeparator2() {
        String invalidSql = "#comment\nselect /* comment */ /*+SET_VAR(disable_join_reorder=true)*/* from    " +
                "tbl where-- comment\n" +
                "col = 1 #comment\r\n" +
                "\tand /*\n" +
                "comment\n" +
                "comment\n" +
                "*/ col = \"con   tent\n" +
                "contend and col = \"''```中\t文  \\\"\r\n\\r\\n\\t\\\"英  文\" and `col`= 'abc\"bcd\\\'';";
        String res = LogUtil.removeCommentAndLineSeparator(invalidSql);
        Assert.assertEquals("select /*+SET_VAR(disable_join_reorder=true)*/* from tbl where col = 1 " +
                "and col = \"con   tent\n" +
                "contend and col = \"''```中\t文  \\\"\r\n\\r\\n\\t\\\"英  文\" and `col`= 'abc\"bcd\\'';`", res);
    }
}
