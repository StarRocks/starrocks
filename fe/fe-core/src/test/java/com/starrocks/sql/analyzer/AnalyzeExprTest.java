// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeExprTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeExpr/" + UUID.randomUUID() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        AnalyzeTestUtil.init();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    /**
     * col->'key' should be translated to function call json_query(col, 'key')
     */
    @Test
    public void testArrowExpr() {
        analyzeSuccess("select v_json->'k1' from tjson");
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
        QueryRelation query = analyzeSuccess(sql);
        Assert.assertTrue(query instanceof ValuesRelation);
        ValuesRelation valueRelation = (ValuesRelation) query;
        Assert.assertEquals(1, valueRelation.getRows().size());
        List<Expr> row = valueRelation.getRow(0);
        ArrowExpr arrow = (ArrowExpr) row.get(0);

        // translate arrow expression
        ScalarOperator so =
                SqlToScalarOperatorTranslator.translate(arrow, new ExpressionMapping(null, Collections.emptyList()));
        Assert.assertEquals(OperatorType.CALL, so.getOpType());
        CallOperator callOperator = (CallOperator) so;
        Assert.assertEquals(expected, callOperator.toString());

    }

}
