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

import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExactNumericConstantCastTest {
    private static final Type DECIMAL = TypeFactory.createDecimalV3NarrowestType(38, 9);

    private static ConstantOperator decimal(String value) {
        return ConstantOperator.createDecimal(new BigDecimal(value), DECIMAL);
    }

    @Test
    public void testIntegralDecimalScalesAndExponents() {
        String[][] values = {
                {"80940.0", "80940"}, {"8.094E4", "80940"}, {"21.0", "21"},
                {"0.000", "0"}, {"-0.000", "0"}, {"-80940.000", "-80940"},
                // An exact decimal must not lose precision through a double intermediate.
                {"9007199254740993.0", "9007199254740993"},
                {"9223372036854775807.0", "9223372036854775807"},
                {"-9223372036854775808.0", "-9223372036854775808"}
        };
        for (String[] value : values) {
            Optional<ScalarOperator> result = Utils.tryCastConstant(decimal(value[0]), IntegerType.BIGINT);
            assertTrue(result.isPresent(), value[0]);
            assertEquals(ConstantOperator.createBigint(Long.parseLong(value[1])), result.get(), value[0]);
        }
    }

    @Test
    public void testRejectFractionAndIntegerOverflow() {
        for (String value : new String[] {"21.1", "-0.1", "9223372036854775808.0", "-9223372036854775809.0"}) {
            assertTrue(Utils.tryCastConstant(decimal(value), IntegerType.BIGINT).isEmpty(), value);
        }
        assertEquals(ConstantOperator.createTinyInt((byte) 127),
                Utils.tryCastConstant(decimal("127.0"), IntegerType.TINYINT).orElseThrow());
        assertEquals(ConstantOperator.createTinyInt((byte) -128),
                Utils.tryCastConstant(decimal("-128.0"), IntegerType.TINYINT).orElseThrow());
        assertTrue(Utils.tryCastConstant(decimal("128.0"), IntegerType.TINYINT).isEmpty());
        assertTrue(Utils.tryCastConstant(decimal("-129.0"), IntegerType.TINYINT).isEmpty());

        assertTrue(Utils.tryCastConstant(ConstantOperator.createLargeInt(BigInteger.ONE.shiftLeft(127)),
                IntegerType.BIGINT).isEmpty());
    }

    @Test
    public void testExactDecimalScaleChange() {
        Type target = TypeFactory.createDecimalV3NarrowestType(9, 2);
        ConstantOperator result = (ConstantOperator) Utils.tryCastConstant(decimal("21.000"), target).orElseThrow();
        assertEquals(target, result.getType());
        assertEquals(0, new BigDecimal("21").compareTo(result.getDecimal()));
        assertTrue(Utils.tryCastConstant(decimal("21.001"), target).isEmpty());
        assertTrue(Utils.tryCastConstant(decimal("10000000.0"), target).isEmpty());
    }

    @Test
    public void testDecimalPrecisionDiffersFromCastReductionStorageRange() {
        Type columnType = TypeFactory.createDecimalV3NarrowestType(13, 0);
        Type wideType = TypeFactory.createDecimalV3NarrowestType(20, 0);
        ColumnRefOperator column = new ColumnRefOperator(1, columnType, "value", false);
        CastOperator widenedColumn = new CastOperator(wideType, column);
        for (String value : new String[] {"9223372000000000000", "-9223372000000000000",
                "9223372036854775807", "-9223372036854775808"}) {
            ConstantOperator source = ConstantOperator.createDecimal(new BigDecimal(value), wideType);
            // New implicit constants must fit the declared precision, even if the storage could hold them.
            assertTrue(Utils.tryCastConstant(source, columnType).isEmpty(), value);
            // Reducing an existing lossless column cast retains the prior physical-range optimization.
            ConstantOperator reduced = (ConstantOperator) Utils.tryDecimalCastConstant(widenedColumn, source)
                    .orElseThrow();
            assertEquals(columnType, reduced.getType());
            assertEquals(0, new BigDecimal(value).compareTo(reduced.getDecimal()), value);
        }
        for (String value : new String[] {"9223372036854775808", "-9223372036854775809"}) {
            ConstantOperator source = ConstantOperator.createDecimal(new BigDecimal(value), wideType);
            assertTrue(Utils.tryDecimalCastConstant(widenedColumn, source).isEmpty(), value);
        }

        Type fractionalType = TypeFactory.createDecimalV3NarrowestType(21, 1);
        CastOperator fractionalColumn = new CastOperator(fractionalType, column);
        ConstantOperator fraction = ConstantOperator.createDecimal(new BigDecimal("21.1"), fractionalType);
        assertTrue(Utils.tryDecimalCastConstant(fractionalColumn, fraction).isEmpty());
        assertEquals(ConstantOperator.createNull(columnType),
                Utils.tryDecimalCastConstant(fractionalColumn, ConstantOperator.createNull(fractionalType))
                        .orElseThrow());
    }

    @Test
    public void testLargeintBounds() {
        Type wideDecimal = TypeFactory.createDecimalV3NarrowestType(76, 0);
        BigInteger min = BigInteger.ONE.shiftLeft(127).negate();
        BigInteger max = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
        for (BigInteger value : new BigInteger[] {min, max}) {
            ConstantOperator source = ConstantOperator.createDecimal(new BigDecimal(value), wideDecimal);
            assertEquals(ConstantOperator.createLargeInt(value),
                    Utils.tryCastConstant(source, IntegerType.LARGEINT).orElseThrow());
        }
        for (BigInteger value : new BigInteger[] {min.subtract(BigInteger.ONE), max.add(BigInteger.ONE)}) {
            ConstantOperator source = ConstantOperator.createDecimal(new BigDecimal(value), wideDecimal);
            assertTrue(Utils.tryCastConstant(source, IntegerType.LARGEINT).isEmpty(), value.toString());
        }
    }

    @Test
    public void testLegacyDecimalV2RemainsConservative() {
        // DECIMALV2's BE representation has a fixed scale of nine, unlike the FE BigDecimal.
        ConstantOperator belowScale = ConstantOperator.createDecimal(new BigDecimal("0.00000000010"),
                TypeFactory.createDecimalV3NarrowestType(11, 11));
        assertTrue(Utils.tryCastConstant(belowScale, DecimalType.DECIMALV2).isEmpty());
    }

    @Test
    public void testNullAndFloatingPointRemainConservative() {
        assertEquals(ConstantOperator.createNull(IntegerType.BIGINT),
                Utils.tryCastConstant(ConstantOperator.createNull(DECIMAL), IntegerType.BIGINT).orElseThrow());
        assertTrue(Utils.tryCastConstant(decimal("9007199254740993.0"), FloatType.DOUBLE).isEmpty());
        assertTrue(Utils.tryCastConstant(ConstantOperator.createDouble(21.5), IntegerType.BIGINT).isEmpty());
        assertTrue(Utils.tryCastConstant(ConstantOperator.createFloat(21), IntegerType.BIGINT).isEmpty());
    }
}
