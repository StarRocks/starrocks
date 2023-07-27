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

import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
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
        List<Expr> row = ((SelectRelation) query).getOutputExpression();
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
        Assert.assertTrue(AstToStringBuilder.toString(statementBase)
                .contains("(test.t0.v2 & (~test.t0.v1)) | (test.t0.v3 ^ 1)"));

        sql = "select v1 * v1 / v1 % v1 + v1 - v1 DIV v1 from t0";
        statementBase = analyzeSuccess(sql);
        Assert.assertTrue(AstToStringBuilder.toString(statementBase)
                .contains("((((test.t0.v1 * test.t0.v1) / test.t0.v1) % test.t0.v1) + test.t0.v1) " +
                        "- (test.t0.v1 DIV test.t0.v1)"));
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
        analyzeSuccess("select array_map((x,y)->((x is null) and (y is null)), [1232], [[['abc']]])");
        analyzeSuccess("select array_map([1], x -> x)");
        analyzeSuccess("select array_map([1], x -> x + v1) from t0");
        analyzeSuccess("select transform([1], x -> x)");
        analyzeSuccess("select arr,array_length(arr) from (select array_map(x->x+1, [1,2]) as arr)T");
        analyzeSuccess("select array_agg(array_length(array_map(x->x*2, v3))) from tarray");
        analyzeSuccess("select array_map(x->x+ array_length(array_agg(v1)),[2,6]) from tarray");
        analyzeSuccess("select array_agg(v1), array_map(x->(array_map((y,z)->y+z, x, array_agg(v1))), [[2,4]]) from tarray");
        analyzeSuccess("select array_map(x->x+12, array_agg(v1)) from tarray");
        analyzeSuccess("select array_map(x->x >  count(v1), v3) from tarray group by v3");
        analyzeSuccess("select array_map(x-> x +  count(v1) over (partition by v1 order by v2),[111]) from tarray");
        analyzeSuccess("select v1, v2, count(v1) over (partition by v1 order by v2) from tarray");
        analyzeSuccess("select v1, v2, count(v1) over (partition by array_sum(array_map(x->x+1, [1])) order by v2) from tarray");
        analyzeSuccess("with x2 as (select array_map((ss) -> ss * v1, v3) from tarray) select * from x2;");
        analyzeSuccess("select array_map(array_map(x2->x2+1,[1,2,3]),array_map(x1->x1+2,[1,2,3]),(x,y)->(x+y))");
        analyzeSuccess("select array_map((x,y,z) -> x is null and y is not null or z is not null, [[1]], [null],['abc'])");


        analyzeFail("select array_map(x,y -> x + y, [], [])"); // should be (x,y)
        analyzeFail("select array_map((x,y,z) -> x + y, [], [])");
        analyzeFail("select array_map(x -> z,[1])");
        analyzeFail("select array_map(x -> x,[1],null)");
        analyzeFail("select arrayMap(x -> x,[1])");
        analyzeFail("select array_map(x -> x+1, 1)");
        analyzeFail("select array_map((x,x) -> x+1, [1],[1])");
        analyzeFail("select array_map((x,y) -> x+1)");
        analyzeFail("select array_map((x,x) -> x+1, [1], x ->x+1)");
        analyzeFail("select array_map()");
        analyzeFail("select array_map(null)");
        analyzeFail("select array_map(null, [1])");
        analyzeFail("select array_map(null, null)");
        analyzeFail("select array_map([1],null);");
        analyzeFail("select array_map(1)");
        analyzeFail("select transform()");
        analyzeFail("select transform(null)");
        analyzeFail("select transform(null, [1])");
        analyzeFail("select transform(null, null)");
        analyzeFail("select transform([1],null);");
        analyzeFail("select transform(1)");
        analyzeFail("select array_map(x->x+ array_length(array_agg(x)),[2,6]) from tarray");
        analyzeFail("select array_map(x->x >  count(v1), v3) from tarray");
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
        analyzeSuccess("select array_filter((x,y)->((x is null) and (y is null)), [1232], [[['abc']]])");
        analyzeSuccess("select array_filter((x,y,z) -> x is null and y is not null or z is not null, [[1]], [null],['abc'])");

        analyzeFail("select array_filter(x,y -> x + y, [], [])"); // should be (x,y)
        analyzeFail("select array_filter((x,y,z) -> x + y, [], [])");
        analyzeFail("select arrayFilter([1], x -> x)");
        analyzeFail("select array_filter(x -> z,[1])");
        analyzeFail("select array_filter(x -> x,[1],null)");
        analyzeFail("select array_filter(1,[2])");
        analyzeFail("select array_filter([],[],[])");
        analyzeFail("select array_filter([2],1)");
    }
    @Test
    public void testLambdaFunctionMapApply() {
        analyzeSuccess("select map_apply((k,v)->(k+1,length(v)), col_map) from " +
                "(select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'']) as col_map " +
                "union all select map_from_arrays(null,null) " +
                "union all select map_from_arrays([],[]) union all select map_from_arrays([null],[null]))A;");
        analyzeSuccess("select map_apply((k,v)->(null,null), col_map) from " +
                "(select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'']) as col_map union all select\n" +
                "map_from_arrays(null,null) union all select map_from_arrays([],[]) " +
                "union all select map_from_arrays([null],[null]))A");
        analyzeSuccess("select map_apply((k,v)->(k,null), col_map) from " +
                "(select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'']) as col_map " +
                "union all select map_from_arrays(null,null) union all select map_from_arrays([],[]) " +
                "union all select map_from_arrays([null],[null]))A;");

        analyzeFail("select map_apply((k,v)->(k+1,length(v)), col_map) from (select null as col_map)A;");
        analyzeFail(" select map_apply((k)->(k,k), col_map) from (select map_from_arrays([1,3,null,2,null]," +
                "['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,k,k)->(k,k), col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,k)->(k,k), col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,v)->(k,v1), col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply(()->(k,k), col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,v)->, col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,v), col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply(null, col_map) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,v)->(v,k), null) from (select map_from_arrays([1,3,null,2," +
                "null],['ab','cdd',null,null,'']) as col_map)A;");
        analyzeFail("select map_apply((k,v)->(v,k), col_map,col_map) from (select map_from_arrays" +
                "([1,3,null,2,null],['ab','cdd',null,null,'']) as col_map)A;");
    }

    @Test
    public void testBinaryLiteral() {
        analyzeSuccess("select x'0000'");
        analyzeSuccess("select x'0000' from tbinary ");
        analyzeSuccess("select x\"0000\" from tbinary ");
        analyzeSuccess("select hex(x'0000') from tbinary ");
        analyzeSuccess("select hex(x\"0000\") from tbinary ");
        analyzeSuccess("select hex(v_varbinary4) from tbinary ");
        analyzeSuccess("select hex(v_varbinary) from tbinary ");
        analyzeSuccess("insert into tbinary values(1, x'0000', x'0000' )");
    }

    @Test
    public void testArrayAgg() {
        analyzeSuccess("select array_agg(v1 order by v2 desc), array_agg(v1 order by v2) from t0;");
        analyzeSuccess("select array_agg(v1 order by v2, v3 desc nulls last) from t0 group by v3;");
        analyzeSuccess("select array_agg(null) from t0;");
        analyzeSuccess("select array_agg(null order by null) from t0;");
        analyzeSuccess("select array_agg(v1 order by null) from t0;");
        analyzeSuccess("select array_agg(v1 order by 1) from t0;");
        analyzeSuccess("select array_agg(null);");
        analyzeSuccess("select array_agg(v1 order by v1) from t0;");
        analyzeSuccess("select array_agg(null order by 11);");
        analyzeSuccess("select array_agg(null order by 1,1);");
        analyzeSuccess("select array_agg(1 order by null,null);");
        analyzeSuccess("select array_agg(1 order by null,null,v1) from t0 group by v2;");
        analyzeSuccess("select array_agg(a order by b) from (select null as a, null as b " +
                "union all select v1 as a, v3 as b from t0)A;");
        analyzeSuccess("select array_agg(v1 order by v1),array_sortby(array_agg(v1),array_agg(v2)) from t0;");
        analyzeSuccess("select array_agg(tj) from tall");
        analyzeSuccess("select array_agg(tj order by ta) from tall group by tc");


        analyzeFail("select array_agg(null order by);");
        analyzeFail("select array_agg(null,'a');");
        analyzeFail("select array_agg(1,1);");
        analyzeFail("select array_agg([1,1]);");
        analyzeFail("select array_agg(1 order by 1 nulls first desc)");
    }
}
