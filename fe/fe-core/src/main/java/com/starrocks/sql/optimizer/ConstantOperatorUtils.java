// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * TYPE            |  JAVA_TYPE
 * TYPE_INVALID    |    null
 * TYPE_NULL       |    null
 * TYPE_BOOLEAN    |    boolean
 * TYPE_TINYINT    |    byte
 * TYPE_SMALLINT   |    short
 * TYPE_INT        |    int
 * TYPE_BIGINT     |    long
 * TYPE_LARGEINT   |    BigInteger
 * TYPE_FLOAT      |    double
 * TYPE_DOUBLE     |    double
 * TYPE_DATE       |    LocalDateTime
 * TYPE_DATETIME   |    LocalDateTime
 * TYPE_TIME       |    LocalDateTime
 * TYPE_DECIMAL    |    BigDecimal
 * TYPE_DECIMALV2  |    BigDecimal
 * TYPE_VARCHAR    |    String
 * TYPE_CHAR       |    String
 * TYPE_HLL        |    NOT_SUPPORT
 * TYPE_BITMAP     |    NOT_SUPPORT
 * TYPE_PERCENTILE |    NOT_SUPPORT
 */
public class ConstantOperatorUtils {

    public static double getDoubleValue(ConstantOperator constantOperator) {
        if (constantOperator.getType().isInt()) {
            return (int) constantOperator.getValue();
        } else if (constantOperator.getType().isTinyint()) {
            return (byte) constantOperator.getValue();
        } else if (constantOperator.getType().isSmallint()) {
            return (short) constantOperator.getValue();
        } else if (constantOperator.getType().isBigint()) {
            return (long) constantOperator.getValue();
        } else if (constantOperator.getType().isBoolean()) {
            return ((boolean) constantOperator.getValue()) == true ? 1 : 0;
        } else if (constantOperator.getType().isDecimalOfAnyVersion()) {
            BigDecimal value = (BigDecimal) constantOperator.getValue();
            return value.doubleValue();
        } else if (constantOperator.getType().isFloatingPointType()) {
            return (double) constantOperator.getValue();
        } else if (constantOperator.getType().isDateType()) {
            return Utils.getLongFromDateTime((LocalDateTime) constantOperator.getValue());
        } else if (constantOperator.getType().isTime()) {
            return (double) constantOperator.getValue();
        } else {
            return Double.NaN;
        }

    }
}
