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
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
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
import mockit.Expectations;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class FoldConstantsRuleTest {
    private final FoldConstantsRule rule = new FoldConstantsRule();

    private static final ConstantOperator OB_TRUE = ConstantOperator.createBoolean(true);
    private static final ConstantOperator OB_FALSE = ConstantOperator.createBoolean(false);
    private static final ConstantOperator OB_NULL = ConstantOperator.createNull(Type.BOOLEAN);

    @Test
    public void applyCall() {

        CallOperator root = new CallOperator(FunctionSet.CONCAT, Type.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3")
        ));

        Function fn =
                new Function(new FunctionName(FunctionSet.CONCAT), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

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
        InPredicateOperator ipo1 = new InPredicateOperator(ConstantOperator.createNull(Type.BOOLEAN));
        assertEquals(OB_NULL, rule.apply(ipo1, null));

        InPredicateOperator ipo2 = new InPredicateOperator(new ColumnRefOperator(1, Type.VARCHAR, "name", true),
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
                ConstantOperator.createNull(Type.BOOLEAN));

        assertEquals(OB_NULL, rule.apply(ipo5, null));

        InPredicateOperator ipo6 = new InPredicateOperator(true, ConstantOperator.createTime(-1077590.0),
                ConstantOperator.createNull(Type.TIME));
        assertEquals(OB_NULL, rule.apply(ipo6, null));
    }

    @Test
    public void applyInNull() {
        InPredicateOperator ipo2 = new InPredicateOperator(ConstantOperator.createInt(0),
                ConstantOperator.createNull(Type.INT));
        assertEquals(ConstantOperator.createNull(Type.BOOLEAN), rule.apply(ipo2, null));
    }

    @Test
    public void applyIsNull() {
        IsNullPredicateOperator inpo1 =
                new IsNullPredicateOperator(new ColumnRefOperator(1, Type.BOOLEAN, "name", true));
        assertEquals(inpo1, rule.apply(inpo1, null));

        IsNullPredicateOperator inpo2 = new IsNullPredicateOperator(ConstantOperator.createNull(Type.BOOLEAN));
        assertEquals(OB_TRUE, rule.apply(inpo2, null));

        IsNullPredicateOperator inpo3 = new IsNullPredicateOperator(true, ConstantOperator.createNull(Type.BOOLEAN));
        assertEquals(OB_FALSE, rule.apply(inpo3, null));
    }

    @Test
    public void applyCast() {
        CastOperator cast1 = new CastOperator(Type.BOOLEAN, ConstantOperator.createNull(Type.NULL));
        assertEquals(OB_NULL, rule.apply(cast1, null));

        CastOperator cast2 = new CastOperator(Type.BOOLEAN, new ColumnRefOperator(1, Type.VARCHAR, "test", true));
        assertEquals(cast2, rule.apply(cast2, null));

        CastOperator cast3 = new CastOperator(Type.BOOLEAN, ConstantOperator.createChar("true"));
        assertEquals(ConstantOperator.createBoolean(true), rule.apply(cast3, null));

        CastOperator cast4 = new CastOperator(Type.VARCHAR, ConstantOperator.createBigint(123));
        assertEquals(ConstantOperator.createVarchar("123"), rule.apply(cast4, null));

        CastOperator cast5 = new CastOperator(Type.DATE, ConstantOperator.createVarchar("2020-02-12 12:23:00"));
        assertEquals(ConstantOperator.createDate(LocalDateTime.of(2020, 2, 12, 0, 0, 0)), rule.apply(cast5, null));

        CastOperator cast6 = new CastOperator(Type.BIGINT, ConstantOperator.createDate(LocalDateTime.now()));
        assertEquals(cast6, rule.apply(cast6, null));

        CastOperator cast7 = new CastOperator(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 1, 1),
                ConstantOperator.createDecimal(BigDecimal.valueOf(0.00008),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 6, 6))
        );
        assertEquals("0.0", rule.apply(cast7, null).toString());
    }

    @Test
    public void applyBinary() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                ConstantOperator.createNull(Type.INT), ConstantOperator.createInt(1));
        assertEquals(OB_NULL, rule.apply(bpo1, null));

        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                new ColumnRefOperator(1, Type.INT, "name", true), ConstantOperator.createInt(1));
        assertEquals(bpo2, rule.apply(bpo2, null));

        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo3, null));

        BinaryPredicateOperator bpo4 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.NE,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0)));
        assertEquals(OB_FALSE, rule.apply(bpo4, null));

        BinaryPredicateOperator bpo5 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                ConstantOperator.createNull(Type.BOOLEAN), ConstantOperator.createNull(Type.BOOLEAN));
        assertEquals(OB_TRUE, rule.apply(bpo5, null));

        BinaryPredicateOperator bpo6 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                ConstantOperator.createNull(Type.BOOLEAN), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo6, null));

        BinaryPredicateOperator bpo7 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo7, null));

        BinaryPredicateOperator bpo8 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo8, null));

        BinaryPredicateOperator bpo9 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_TRUE, rule.apply(bpo9, null));

        BinaryPredicateOperator bpo10 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertEquals(OB_FALSE, rule.apply(bpo10, null));

    }
}