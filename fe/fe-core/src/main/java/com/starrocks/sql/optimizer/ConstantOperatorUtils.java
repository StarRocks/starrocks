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


package com.starrocks.sql.optimizer;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.OptionalDouble;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

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
        OptionalDouble optionalDouble = doubleValueFromConstant(constantOperator);
        if (optionalDouble.isPresent()) {
            return optionalDouble.getAsDouble();
        } else {
            return Double.NaN;
        }
    }

    public static OptionalDouble doubleValueFromConstant(ConstantOperator constantOperator) {
        if (Type.BOOLEAN.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getBoolean() ? 1.0 : 0.0);
        } else if (Type.TINYINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getTinyInt());
        } else if (Type.SMALLINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getSmallint());
        } else if (Type.INT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getInt());
        } else if (Type.BIGINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getBigint());
        } else if (Type.LARGEINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getLargeInt().doubleValue());
        } else if (Type.FLOAT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getFloat());
        } else if (Type.DOUBLE.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getDouble());
        } else if (Type.DATE.equals(constantOperator.getType())) {
            return OptionalDouble.of(getLongFromDateTime(constantOperator.getDate()));
        } else if (Type.DATETIME.equals(constantOperator.getType())) {
            return OptionalDouble.of(getLongFromDateTime(constantOperator.getDatetime()));
        } else if (Type.TIME.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getTime());
        } else if (constantOperator.getType().isDecimalOfAnyVersion()) {
            return OptionalDouble.of(constantOperator.getDecimal().doubleValue());
        }
        return OptionalDouble.empty();
    }
}
