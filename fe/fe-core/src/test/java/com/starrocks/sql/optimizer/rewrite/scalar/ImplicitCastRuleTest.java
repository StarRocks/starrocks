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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.Month;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImplicitCastRuleTest {

    @Test
    public void testCall() {
        Function fn = new Function(new FunctionName("add"), new Type[] {Type.BIGINT, Type.BIGINT}, Type.BIGINT, true);

        CallOperator op = new CallOperator("add", Type.BIGINT, Lists.newArrayList(
                ConstantOperator.createVarchar("1"),
                ConstantOperator.createBoolean(true),
                ConstantOperator.createBigint(1),
                ConstantOperator.createInt(1)
        ));

        new Expectations(op) {{
                op.getFunction();
                minTimes = 0;
                result = fn;
            }};

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.apply(op, null);

        assertEquals(OperatorType.CALL, result.getOpType());
        assertEquals(4, result.getChildren().size());

        assertTrue(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(1) instanceof CastOperator);
        assertFalse(result.getChild(2) instanceof CastOperator);
        assertTrue(result.getChild(3) instanceof CastOperator);

        assertEquals(Type.BIGINT, result.getChild(0).getType());
        assertEquals(Type.BIGINT, result.getChild(1).getType());
        assertEquals(Type.BIGINT, result.getChild(2).getType());
        assertEquals(Type.BIGINT, result.getChild(3).getType());

        assertEquals(Type.VARCHAR, result.getChild(0).getChild(0).getType());
        assertEquals(Type.BOOLEAN, result.getChild(1).getChild(0).getType());
        assertEquals(Type.INT, result.getChild(3).getChild(0).getType());
    }

    @Test
    public void testBetweenPredicate() {
        BetweenPredicateOperator op = new BetweenPredicateOperator(false, ConstantOperator.createBigint(1),
                ConstantOperator.createVarchar("1"), ConstantOperator.createDate(LocalDateTime.now()));

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.apply(op, null);

        assertTrue(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(1) instanceof CastOperator);
        assertTrue(result.getChild(2) instanceof CastOperator);

        assertEquals(Type.DOUBLE, result.getChild(0).getType());
        assertEquals(Type.DOUBLE, result.getChild(1).getType());
        assertEquals(Type.DOUBLE, result.getChild(2).getType());

        assertEquals(Type.VARCHAR, result.getChild(1).getChild(0).getType());
        assertEquals(Type.DATE, result.getChild(2).getChild(0).getType());
    }

    @Test
    public void testBinaryPredicate() {
        BinaryPredicateOperator op =
                new BinaryPredicateOperator(BinaryType.EQ,
                        ConstantOperator.createVarchar("1"), ConstantOperator.createInt(1));

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.apply(op, null);

        assertTrue(result.getChild(0) instanceof ConstantOperator);
        assertTrue(result.getChild(1) instanceof CastOperator);

        assertEquals(PrimitiveType.VARCHAR, result.getChild(0).getType().getPrimitiveType());
        assertEquals(PrimitiveType.VARCHAR, result.getChild(1).getType().getPrimitiveType());

        assertTrue(result.getChild(1).getChild(0).getType().isInt());
    }

    @Test
    public void testEqualForNullBinaryPredicateWithConstNull() {
        BinaryPredicateOperator[] ops = new BinaryPredicateOperator[] {
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createVarchar("a"),
                        ConstantOperator.createNull(Type.DOUBLE)),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createNull(Type.DOUBLE),
                        ConstantOperator.createVarchar("a")),
        };
        for (BinaryPredicateOperator op : ops) {
            ImplicitCastRule rule = new ImplicitCastRule();
            ScalarOperator result = rule.apply(op, null);

            assertTrue(result.getChild(0) instanceof ConstantOperator);
            assertTrue(result.getChild(1) instanceof ConstantOperator);

            assertEquals(PrimitiveType.VARCHAR, result.getChild(0).getType().getPrimitiveType());
            assertEquals(PrimitiveType.VARCHAR, result.getChild(1).getType().getPrimitiveType());
        }
    }

    @Test
    public void testEqualForNullBinaryPredicateWithoutConstNull() {
        BinaryPredicateOperator[] ops = new BinaryPredicateOperator[] {
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createDate(LocalDateTime.of(2022, Month.JANUARY, 01, 0, 0, 0)),
                        new ColumnRefOperator(1, Type.DATE, "date_col", true)
                ),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        new ColumnRefOperator(1, Type.DATE, "date_col", true),
                        ConstantOperator.createInt(20220111)
                ),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createVarchar("2022-01-01"),
                        new ColumnRefOperator(1, Type.DATE, "date_col", true)
                ),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        new ColumnRefOperator(1, Type.DATE, "date_col", true),
                        ConstantOperator.createVarchar("2022-01-01")
                ),
        };
        for (BinaryPredicateOperator op : ops) {
            ImplicitCastRule rule = new ImplicitCastRule();
            ScalarOperator result = rule.apply(op, null);

            assertEquals(PrimitiveType.DATE, result.getChild(0).getType().getPrimitiveType());
            assertEquals(PrimitiveType.DATE, result.getChild(1).getType().getPrimitiveType());
        }
    }

    @Test
    public void testCompoundPredicate() {
        CompoundPredicateOperator op =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        ConstantOperator.createVarchar("1"), ConstantOperator.createInt(1));

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.apply(op, null);

        assertTrue(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(1) instanceof CastOperator);

        assertTrue(result.getChild(0).getType().isBoolean());
        assertTrue(result.getChild(1).getType().isBoolean());

        assertTrue(result.getChild(0).getChild(0).getType().isVarchar());
        assertTrue(result.getChild(1).getChild(0).getType().isInt());
    }

    @Test
    public void testInPredicate() {
        InPredicateOperator op = new InPredicateOperator(ConstantOperator.createBigint(1),
                ConstantOperator.createVarchar("1"), ConstantOperator.createDate(LocalDateTime.now()));

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.apply(op, null);

        assertTrue(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(1) instanceof CastOperator);
        assertTrue(result.getChild(2) instanceof CastOperator);

        assertEquals(Type.DOUBLE, result.getChild(0).getType());
        assertEquals(Type.DOUBLE, result.getChild(1).getType());
        assertEquals(Type.DOUBLE, result.getChild(2).getType());

        assertTrue(result.getChild(1).getChild(0).getType().isVarchar());
        assertTrue(result.getChild(2).getChild(0).getType().isDate());
    }

    @Test
    public void testLikePredicateOperator() {
        LikePredicateOperator op =
                new LikePredicateOperator(ConstantOperator.createVarchar("1"), ConstantOperator.createInt(1));

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.apply(op, null);

        assertFalse(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(1) instanceof CastOperator);

        assertTrue(result.getChild(0).getType().isVarchar());
        assertTrue(result.getChild(1).getType().isVarchar());

        assertTrue(result.getChild(1).getChild(0).getType().isInt());
    }
}