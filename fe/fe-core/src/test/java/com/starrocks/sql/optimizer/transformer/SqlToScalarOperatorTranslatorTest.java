// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.analysis.DateLiteral;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SqlToScalarOperatorTranslatorTest {

    @Test
    public void TestTranslateConstant() {
        DateLiteral literal = new DateLiteral(2000, 12, 1);
        ScalarOperator so =
                SqlToScalarOperatorTranslator.translate(literal, new ExpressionMapping(null, Collections.emptyList()));

        assertEquals(OperatorType.CONSTANT, so.getOpType());
        assertEquals(LocalDateTime.of(2000, 12, 1, 0, 0), ((ConstantOperator) so).getDate());
    }
}
