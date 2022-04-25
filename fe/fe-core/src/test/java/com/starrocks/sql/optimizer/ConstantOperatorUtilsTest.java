package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConstantOperatorUtilsTest {


    @Test
    public void getDoubleValue() {

        ConstantOperator CONSTANT_0 = ConstantOperator.createTinyInt((byte) 1);
        ConstantOperator CONSTANT_1 = ConstantOperator.createInt(1000);
        ConstantOperator CONSTANT_2 = ConstantOperator.createSmallInt((short) 12);
        ConstantOperator CONSTANT_3 = ConstantOperator.createBigint(1000000);

        ConstantOperator CONSTANT_4 = ConstantOperator.createFloat(1.5);
        ConstantOperator CONSTANT_5 = ConstantOperator.createDouble(6.789);

        ConstantOperator CONSTANT_6 = ConstantOperator.createBoolean(true);
        ConstantOperator CONSTANT_7 = ConstantOperator.createBoolean(false);

        ConstantOperator CONSTANT_8 = ConstantOperator.createDate(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator CONSTANT_9 = ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator CONSTANT_10 = ConstantOperator.createTime(124578990d);

        ConstantOperator CONSTANT_11 = ConstantOperator.createVarchar("123");

        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_0), 1, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_1), 1000, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_2), 12, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_3), 1000000, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_4), 1.5, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_5), 6.789, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_6), 1, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_7), 0, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_8), 1065887785, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_9), 1065887785, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_10), 124578990, 0.1);

        assertEquals(ConstantOperatorUtils.getDoubleValue(CONSTANT_11), Double.NaN, 0.0);

    }

}
