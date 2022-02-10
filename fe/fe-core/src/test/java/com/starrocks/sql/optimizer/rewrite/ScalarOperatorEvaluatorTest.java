// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScalarOperatorEvaluatorTest {
    @Test
    public void evaluationNotConstant() {
        CallOperator operator = new CallOperator("ifnull", Type.INT,
                Lists.newArrayList(new ColumnRefOperator(1, Type.INT, "test", true), ConstantOperator.createInt(2)));

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

    @Test
    public void evaluationNull() {
        CallOperator operator = new CallOperator("concat", Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createNull(Type.VARCHAR)));

        Function fn = new Function(new FunctionName("concat"), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertTrue(((ConstantOperator) result).isNull());
    }

    @Test
    public void evaluationArrayArgs() {
        CallOperator operator = new CallOperator("concat", Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createVarchar("123")));

        Function fn = new Function(new FunctionName("concat"), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertEquals("test123", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void evaluationUtc() throws AnalysisException {
        CallOperator operator = new CallOperator("utc_timestamp", Type.VARCHAR, Lists.newArrayList());

        Function fn = new Function(new FunctionName("utc_timestamp"), new Type[] {}, Type.DATETIME, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES),
                ((ConstantOperator) result).getDatetime().truncatedTo(ChronoUnit.MINUTES));
    }

    @Test
    public void evaluationFromUtc() {
        CallOperator operator = new CallOperator("str_to_date", Type.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("2003-10-11 23:56:25"),
                ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")
        ));

        Function fn =
                new Function(new FunctionName("str_to_date"), new Type[] {Type.VARCHAR, Type.VARCHAR}, Type.DATETIME,
                        false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(LocalDateTime.of(2003, 10, 11, 23, 56, 25), ((ConstantOperator) result).getDatetime());
    }

    @Test
    public void evaluationNonNullableFunc() {
        CallOperator operator = new CallOperator("bitmap_count", Type.BIGINT,
                Lists.newArrayList(ConstantOperator.createNull(Type.BITMAP)));

        Function fn = new Function(new FunctionName("bitmap_count"), new Type[] {Type.BITMAP}, Type.BIGINT, false);
        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

}