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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FoldConstantsRuleTest {
    private final FoldConstantsRule rule = new FoldConstantsRule();

    private static final ConstantOperator OB_TRUE = ConstantOperator.createBoolean(true);
    private static final ConstantOperator OB_FALSE = ConstantOperator.createBoolean(false);
    private static final ConstantOperator OB_NULL = ConstantOperator.createNull(BooleanType.BOOLEAN);

    @Test
    public void applyCall() {

        CallOperator root = new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3")
        ));

        Function fn = new Function(new FunctionName(FunctionSet.CONCAT),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(root) {
            {
                root.getFunction();
                result = fn;
            }
        };

        ScalarOperator operator = rule.apply(root, new ScalarOperatorRewriteContext());

        assertEquals(OperatorType.CONSTANT, operator.getOpType());
        assertEquals("123", ((ConstantOperator) operator).getVarchar());
    }

    @Test
    public void applyIn() {
        InPredicateOperator ipo1 = new InPredicateOperator(ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_NULL, rule.apply(ipo1, null));

        InPredicateOperator ipo2 = new InPredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("test"));
        assertEquals(ipo2, rule.apply(ipo2, null));

        InPredicateOperator ipo3 = new InPredicateOperator(ConstantOperator.createVarchar("test1"),
                ConstantOperator.createVarchar("test2"),
                ConstantOperator.createVarchar("test3"),
                ConstantOperator.createVarchar("test4"),
                ConstantOperator.createVarchar("test1"));
        assertEquals(OB_TRUE, rule.apply(ipo3, null));

        InPredicateOperator ipo4 = new InPredicateOperator(true, ConstantOperator.createVarchar("test1"),
                ConstantOperator.createVarchar("test2"),
                ConstantOperator.createVarchar("test3"),
                ConstantOperator.createVarchar("test4"),
                ConstantOperator.createVarchar("test1"));
        assertEquals(OB_FALSE, rule.apply(ipo4, null));

        InPredicateOperator ipo5 = new InPredicateOperator(true, ConstantOperator.createVarchar("test1"),
                ConstantOperator.createVarchar("test2"),
                ConstantOperator.createVarchar("test3"),
                ConstantOperator.createVarchar("test4"),
                ConstantOperator.createNull(BooleanType.BOOLEAN));

        assertEquals(OB_NULL, rule.apply(ipo5, null));

        InPredicateOperator ipo6 = new InPredicateOperator(true, ConstantOperator.createTime(-1077590.0),
                ConstantOperator.createNull(DateType.TIME));
        assertEquals(OB_NULL, rule.apply(ipo6, null));
    }

    @Test
    public void applyInNull() {
        InPredicateOperator ipo2 = new InPredicateOperator(ConstantOperator.createInt(0),
                ConstantOperator.createNull(IntegerType.INT));
        assertEquals(ConstantOperator.createNull(BooleanType.BOOLEAN), rule.apply(ipo2, null));
    }

    @Test
    public void applyIsNull() {
        IsNullPredicateOperator inpo1 =
                new IsNullPredicateOperator(new ColumnRefOperator(1, BooleanType.BOOLEAN, "name", true));
        assertEquals(inpo1, rule.apply(inpo1, null));

        IsNullPredicateOperator inpo2 = new IsNullPredicateOperator(ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_TRUE, rule.apply(inpo2, null));

        IsNullPredicateOperator inpo3 = new IsNullPredicateOperator(true, ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_FALSE, rule.apply(inpo3, null));
    }

    @Test
    public void applyCast() {
        CastOperator cast1 = new CastOperator(BooleanType.BOOLEAN, ConstantOperator.createNull(NullType.NULL));
        assertEquals(OB_NULL, rule.apply(cast1, null));

        CastOperator cast2 = new CastOperator(BooleanType.BOOLEAN,
                new ColumnRefOperator(1, VarcharType.VARCHAR, "test", true));
        assertEquals(cast2, rule.apply(cast2, null));

        CastOperator cast3 = new CastOperator(BooleanType.BOOLEAN, ConstantOperator.createChar("true"));
        assertEquals(ConstantOperator.createBoolean(true), rule.apply(cast3, null));

        CastOperator cast4 = new CastOperator(VarcharType.VARCHAR, ConstantOperator.createBigint(123));
        assertEquals(ConstantOperator.createVarchar("123"), rule.apply(cast4, null));

        CastOperator cast5 = new CastOperator(DateType.DATE, ConstantOperator.createVarchar("2020-02-12 12:23:00"));
        assertEquals(ConstantOperator.createDate(LocalDateTime.of(2020, 2, 12, 0, 0, 0)), rule.apply(cast5, null));

        CastOperator cast6 = new CastOperator(
                IntegerType.BIGINT, ConstantOperator.createDate(LocalDateTime.of(2020, 2, 12, 0, 0, 0)));
        assertEquals(ConstantOperator.createBigint(20200212), rule.apply(cast6, null));

        CastOperator cast7 = new CastOperator(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 1, 1),
                ConstantOperator.createDecimal(BigDecimal.valueOf(0.00008),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 6, 6))
        );
        assertEquals("0.0", rule.apply(cast7, null).toString());

        // cast(96.1 as int)-> 96
        CastOperator cast8 = new CastOperator(IntegerType.INT, ConstantOperator.createDouble(96.1));
        assertEquals(ConstantOperator.createInt(96), rule.apply(cast8, null));
    }

    @Test
    public void applyBinary() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createNull(IntegerType.INT), ConstantOperator.createInt(1));
        assertEquals(OB_NULL, rule.apply(bpo1, null));

        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "name", true), ConstantOperator.createInt(1));
        assertEquals(bpo2, rule.apply(bpo2, null));

        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo3, null));

        BinaryPredicateOperator bpo4 = new BinaryPredicateOperator(BinaryType.NE,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0)));
        assertEquals(OB_FALSE, rule.apply(bpo4, null));

        BinaryPredicateOperator bpo5 = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                ConstantOperator.createNull(BooleanType.BOOLEAN), ConstantOperator.createNull(BooleanType.BOOLEAN));
        assertEquals(OB_TRUE, rule.apply(bpo5, null));

        BinaryPredicateOperator bpo6 = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                ConstantOperator.createNull(BooleanType.BOOLEAN), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo6, null));

        BinaryPredicateOperator bpo7 = new BinaryPredicateOperator(BinaryType.GE,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo7, null));

        BinaryPredicateOperator bpo8 = new BinaryPredicateOperator(BinaryType.GT,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo8, null));

        BinaryPredicateOperator bpo9 = new BinaryPredicateOperator(BinaryType.LE,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo9, null));

        BinaryPredicateOperator bpo10 = new BinaryPredicateOperator(BinaryType.LT,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo10, null));
    }

    @Test
    public void applyCastEdgeCases() {
        // 1. Overflow Tests
        // Cast '128' as TINYINT (Max 127) -> No fold (returns original)
        CastOperator castOverflow1 = new CastOperator(IntegerType.TINYINT, ConstantOperator.createVarchar("128"));
        assertEquals(castOverflow1, rule.apply(castOverflow1, null));

        // Cast '32768' as SMALLINT (Max 32767) -> No fold
        CastOperator castOverflow2 = new CastOperator(IntegerType.SMALLINT, ConstantOperator.createVarchar("32768"));
        assertEquals(castOverflow2, rule.apply(castOverflow2, null));

        // Cast '2147483648' as INT (Max 2147483647) -> No fold
        CastOperator castOverflow3 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("2147483648"));
        assertEquals(castOverflow3, rule.apply(castOverflow3, null));

        // Cast '9223372036854775808' as BIGINT -> No fold
        CastOperator castOverflow4 = new CastOperator(IntegerType.BIGINT, ConstantOperator.createVarchar("9223372036854775808"));
        assertEquals(castOverflow4, rule.apply(castOverflow4, null));

        // 2. Underflow Tests
        // Cast '-129' as TINYINT (Min -128) -> No fold
        CastOperator castUnderflow1 = new CastOperator(IntegerType.TINYINT, ConstantOperator.createVarchar("-129"));
        assertEquals(castUnderflow1, rule.apply(castUnderflow1, null));

        // Cast '-2147483649' as INT -> No fold
        CastOperator castUnderflow2 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("-2147483649"));
        assertEquals(castUnderflow2, rule.apply(castUnderflow2, null));

        // 3. Invalid Number Formats
        // 'abc' -> No fold
        CastOperator castInvalid1 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("abc"));
        assertEquals(castInvalid1, rule.apply(castInvalid1, null));

        // '123a' -> No fold
        CastOperator castInvalid2 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("123a"));
        assertEquals(castInvalid2, rule.apply(castInvalid2, null));

        // '12.3.4' -> No fold
        CastOperator castInvalid3 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("12.3.4"));
        assertEquals(ConstantOperator.createInt(12), rule.apply(castInvalid3, null));

        // 4. Truncation & Formatting
        // '123.999' -> 123 (Truncated at dot)
        CastOperator castTrunc1 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("123.999"));
        assertEquals(ConstantOperator.createInt(123), rule.apply(castTrunc1, null));

        // '   123   ' -> 123 (Trimmed)
        CastOperator castTrim1 = new CastOperator(IntegerType.INT, ConstantOperator.createVarchar("   123   "));
        assertEquals(ConstantOperator.createInt(123), rule.apply(castTrim1, null));

        // 5. Boundary Success Cases
        // '127' as TINYINT -> 127
        CastOperator castBound1 = new CastOperator(IntegerType.TINYINT, ConstantOperator.createVarchar("127"));
        assertEquals(ConstantOperator.createTinyInt((byte) 127), rule.apply(castBound1, null));

        // '-128' as TINYINT -> -128
        CastOperator castBound2 = new CastOperator(IntegerType.TINYINT, ConstantOperator.createVarchar("-128"));
        assertEquals(ConstantOperator.createTinyInt((byte) -128), rule.apply(castBound2, null));
    }
}