// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.StatementBase;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeExprTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    /**
     * col->'key' should be translated to function call json_query(col, 'key')
     */
    @Test
    public void testArrowExpr() {
        analyzeSuccess("select v_json->'k1' from tjson");
        // Test for qualified name.
        analyzeSuccess("select tjson.v_json->'k1' from tjson");
        analyzeSuccess("select test.tjson.v_json->'k1' from tjson");

        analyzeSuccess("select v_json->'k1'->'k2' from tjson");
        analyzeSuccess("select parse_json('{\"a\": 1}')->'k1'");

        analyzeFail("select v_int -> 'k1' from tjson");
        analyzeFail("select v_json -> 1 from tjson");
        analyzeFail("select v_json -> k1 from tjson");
    }

    @Test
    public void testTranslateArrowExprForValue() {
        // NOTE quotes will be removed in toString
        testTranslateArrowExprForValue("select parse_json('{\"a\": 1}')->'k1'",
                "json_query(parse_json({\"a\": 1}), k1)");
    }

    private void testTranslateArrowExprForValue(String sql, String expected) {
        QueryRelation query = ((QueryStatement) analyzeSuccess(sql)).getQueryRelation();
        List<Expr> row = ((SelectRelation) query).getOutputExpr();
        ArrowExpr arrow = (ArrowExpr) row.get(0);

        // translate arrow expression
        ScalarOperator so =
                SqlToScalarOperatorTranslator.translate(arrow, new ExpressionMapping(null, Collections.emptyList()),
                        new ColumnRefFactory());
        Assert.assertEquals(OperatorType.CALL, so.getOpType());
        CallOperator callOperator = (CallOperator) so;
        Assert.assertEquals(expected, callOperator.toString());
    }

    @Test
    public void testQuotedToString() {
        QueryRelation query = ((QueryStatement) analyzeSuccess(
                " select (select 1 as v),v1 from t0")).getQueryRelation();
        Assert.assertEquals("(SELECT 1 AS v),v1", String.join(",", query.getColumnOutputNames()));
    }

    @Test
    public void testExpressionPreceding() {
        String sql = "select v2&~v1|v3^1 from t0";
        StatementBase statementBase = analyzeSuccess(sql);
        Assert.assertTrue(AST2SQL.toString(statementBase).contains("(v2 & (~v1)) | (v3 ^ 1)"));

        sql = "select v1 * v1 / v1 % v1 + v1 - v1 DIV v1 from t0";
        statementBase = analyzeSuccess(sql);
        Assert.assertTrue(AST2SQL.toString(statementBase).contains("((((v1 * v1) / v1) % v1) + v1) - (v1 DIV v1)"));
    }

    @Test
    public void testLambdaFunction() {
        analyzeSuccess("select array_map(x -> x,[])");
        analyzeSuccess("select array_map(x -> x,[null])");
        analyzeSuccess("select array_map(x -> x,[1])");
        analyzeSuccess("select array_map(x -> x is null,null)");
        analyzeSuccess("select array_map(x -> array_map(y-> array_map(z -> z + array_length(x),y),x), [[[1,23],[4,3,2]],[[3]]])");
        analyzeSuccess("select array_map(x -> x is null,[null]),array_map(x -> x is null,null)");
        analyzeSuccess("select array_map((x,y) -> x + y, [], [])");
        analyzeSuccess("select array_map((x,y) -> x, [], [])");
        analyzeSuccess("select array_map([1], x -> x)");
        analyzeSuccess("select array_map([1], x -> x + v1) from t0");
        analyzeSuccess("select transform([1], x -> x)");

        analyzeFail("select array_map(x,y -> x + y, [], [])"); // should be (x,y)
        analyzeFail("select array_map((x,y,z) -> x + y, [], [])");
        analyzeFail("select array_map(x -> z,[1])");
        analyzeFail("select array_map(x -> x,[1],null)");
        analyzeFail("select arrayMap(x -> x,[1])");
        analyzeFail("select array_map(x -> x+1, 1)");
        analyzeFail("select array_map((x,x) -> x+1, [1],[1])");
        analyzeFail("select array_map((x,y) -> x+1)");
        analyzeFail("select array_map((x,x) -> x+1, [1], x ->x+1)");
    }

    @Test
    public void testLambdaFunctionArrayFilter() {
        analyzeSuccess("select array_filter(x -> x,[])");
        analyzeSuccess("select array_filter(x -> x,[null])");
        analyzeSuccess("select array_filter(x -> x,[1])");
        analyzeSuccess("select array_filter(x -> x is null,null)");
        analyzeSuccess("select array_filter(x -> x is null,[null]),array_map(x -> x is null,null)");
        analyzeSuccess("select array_filter((x,y) -> x + y, [], [])");
        analyzeSuccess("select array_filter((x,y) -> x, [], [])");

        analyzeFail("select array_filter(x,y -> x + y, [], [])"); // should be (x,y)
        analyzeFail("select array_filter((x,y,z) -> x + y, [], [])");
        analyzeFail("select arrayFilter([1], x -> x)");
        analyzeFail("select array_filter(x -> z,[1])");
        analyzeFail("select array_filter(x -> x,[1],null)");
        analyzeFail("select array_filter(1,[2])");
        analyzeFail("select array_filter([],[],[])");
        analyzeFail("select array_filter([2],1)");
    }
}
