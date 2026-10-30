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
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BinaryType;
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
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImplicitCastRuleTest {

    @Test
    public void testCall() {
        Function fn = new Function(new FunctionName("add"), new Type[] {
                IntegerType.BIGINT, IntegerType.BIGINT}, IntegerType.BIGINT, true);

        CallOperator op = new CallOperator("add", IntegerType.BIGINT, Lists.newArrayList(
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

        assertEquals(IntegerType.BIGINT, result.getChild(0).getType());
        assertEquals(IntegerType.BIGINT, result.getChild(1).getType());
        assertEquals(IntegerType.BIGINT, result.getChild(2).getType());
        assertEquals(IntegerType.BIGINT, result.getChild(3).getType());

        assertEquals(VarcharType.VARCHAR, result.getChild(0).getChild(0).getType());
        assertEquals(BooleanType.BOOLEAN, result.getChild(1).getChild(0).getType());
        assertEquals(IntegerType.INT, result.getChild(3).getChild(0).getType());
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

        assertEquals(FloatType.DOUBLE, result.getChild(0).getType());
        assertEquals(FloatType.DOUBLE, result.getChild(1).getType());
        assertEquals(FloatType.DOUBLE, result.getChild(2).getType());

        assertEquals(VarcharType.VARCHAR, result.getChild(1).getChild(0).getType());
        assertEquals(DateType.DATE, result.getChild(2).getChild(0).getType());
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
                        ConstantOperator.createNull(FloatType.DOUBLE)),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createNull(FloatType.DOUBLE),
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
                        new ColumnRefOperator(1, DateType.DATE, "date_col", true)
                ),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        new ColumnRefOperator(1, DateType.DATE, "date_col", true),
                        ConstantOperator.createInt(20220111)
                ),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createVarchar("2022-01-01"),
                        new ColumnRefOperator(1, DateType.DATE, "date_col", true)
                ),
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        new ColumnRefOperator(1, DateType.DATE, "date_col", true),
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
        assertTrue(result.getChild(1) instanceof ConstantOperator);
        assertTrue(result.getChild(2) instanceof CastOperator);

        assertEquals(VarcharType.VARCHAR, result.getChild(0).getType());
        assertEquals(VarcharType.VARCHAR, result.getChild(1).getType());
        assertEquals(VarcharType.VARCHAR, result.getChild(2).getType());

        assertTrue(result.getChild(0).getChild(0).getType().isBigint());
        assertTrue(result.getChild(2).getChild(0).getType().isDate());

        // date_format(str_to_date('20241209','%Y%m%d'),'%Y-%m-%d 00:00:00') in (str_to_date('20241209','%Y%m%d'),str_to_date('20241208','%Y%m%d'))
        InPredicateOperator op1 = new InPredicateOperator(ConstantOperator.createVarchar("2024-12-09 00:00:00"),
                ConstantOperator.createDate(LocalDate.of(2024, 12, 9).atStartOfDay()),
                ConstantOperator.createDate(LocalDateTime.now()));
        ScalarOperator result1 = rule.apply(op1, null);
        assertTrue(result1.getChild(0).getType().isDateType());
        assertTrue(result1.getChild(1).getType().isDateType());
        assertTrue(result1.getChild(2).getType().isDateType());

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

    @Test
    public void testForbidInvalidImplicitCastMode() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.getSessionVariable().setSqlMode(SqlModeHelper.MODE_FORBID_INVALID_IMPLICIT_CAST);
        ctx.setThreadLocalInfo();
        try {
            ImplicitCastRule rule = new ImplicitCastRule();

            // Cross-family cast (varchar -> int) must be rejected even though
            // optimizeConstantAndVariable would otherwise fold the literal
            // into an INT constant without ever building a CastOperator.
            BinaryPredicateOperator crossFamily = new BinaryPredicateOperator(BinaryType.EQ,
                    new ColumnRefOperator(1, IntegerType.INT, "c_int", true),
                    ConstantOperator.createVarchar("1"));
            assertThrows(SemanticException.class, () -> rule.apply(crossFamily, null));

            // Same-family widening (int -> bigint) must still be allowed.
            BinaryPredicateOperator sameFamily = new BinaryPredicateOperator(BinaryType.EQ,
                    new ColumnRefOperator(2, IntegerType.BIGINT, "c_bigint", true),
                    ConstantOperator.createInt(1));
            ScalarOperator result = rule.apply(sameFamily, null);
            assertTrue(result.getChild(0).getType().isBigint());
            assertTrue(result.getChild(1).getType().isBigint());
        } finally {
            ConnectContext.remove();
        }
    }

    private static ConnectContext enterStrictMode() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.getSessionVariable().setSqlMode(SqlModeHelper.MODE_FORBID_INVALID_IMPLICIT_CAST);
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private static void assertImplicitCastAccepts(Type from, Type to) {
        Function fn = new Function(new FunctionName("coverage_fn"), new Type[] {to}, to, false);
        CallOperator op = new CallOperator("coverage_fn", to,
                Lists.newArrayList(new ColumnRefOperator(1, from, "c", true)), fn);
        ScalarOperator rewritten = new ImplicitCastRule().apply(op, null);
        assertEquals(OperatorType.CALL, rewritten.getOpType());
        // The argument should be wrapped in a CastOperator converging on `to`.
        assertTrue(rewritten.getChild(0) instanceof CastOperator);
        assertEquals(to, rewritten.getChild(0).getType());
    }

    private static void assertImplicitCastRejects(Type from, Type to) {
        Function fn = new Function(new FunctionName("coverage_fn"), new Type[] {to}, to, false);
        CallOperator op = new CallOperator("coverage_fn", to,
                Lists.newArrayList(new ColumnRefOperator(1, from, "c", true)), fn);
        assertThrows(SemanticException.class, () -> new ImplicitCastRule().apply(op, null));
    }

    @Test
    public void testForbidInvalidImplicitCastModeNumericWidening() {
        enterStrictMode();
        try {
            // Walk every rank step in the Trino-like numeric hierarchy.
            assertImplicitCastAccepts(IntegerType.TINYINT, IntegerType.SMALLINT);
            assertImplicitCastAccepts(IntegerType.SMALLINT, IntegerType.INT);
            assertImplicitCastAccepts(IntegerType.INT, IntegerType.BIGINT);
            assertImplicitCastAccepts(IntegerType.BIGINT, IntegerType.LARGEINT);
            assertImplicitCastAccepts(IntegerType.LARGEINT, DecimalType.DEFAULT_DECIMAL128);
            assertImplicitCastAccepts(DecimalType.DEFAULT_DECIMAL128, FloatType.FLOAT);
            assertImplicitCastAccepts(FloatType.FLOAT, FloatType.DOUBLE);
            // Non-adjacent widening also passes.
            assertImplicitCastAccepts(IntegerType.TINYINT, FloatType.DOUBLE);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeNumericNarrowing() {
        enterStrictMode();
        try {
            assertImplicitCastRejects(IntegerType.SMALLINT, IntegerType.TINYINT);
            assertImplicitCastRejects(IntegerType.INT, IntegerType.SMALLINT);
            assertImplicitCastRejects(IntegerType.BIGINT, IntegerType.INT);
            assertImplicitCastRejects(IntegerType.LARGEINT, IntegerType.BIGINT);
            assertImplicitCastRejects(DecimalType.DEFAULT_DECIMAL128, IntegerType.BIGINT);
            assertImplicitCastRejects(FloatType.FLOAT, DecimalType.DEFAULT_DECIMAL64);
            assertImplicitCastRejects(FloatType.DOUBLE, FloatType.FLOAT);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeDecimalWidening() {
        enterStrictMode();
        try {
            // DECIMAL32(9,3) -> DECIMAL64(18,6): rank equal, precision & scale both grow.
            assertImplicitCastAccepts(DecimalType.DEFAULT_DECIMAL32, DecimalType.DEFAULT_DECIMAL64);
            assertImplicitCastAccepts(DecimalType.DEFAULT_DECIMAL64, DecimalType.DEFAULT_DECIMAL128);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeDecimalNarrowing() {
        enterStrictMode();
        try {
            // DECIMAL128(38,9) -> DECIMAL32(9,3): both precision and scale shrink.
            assertImplicitCastRejects(DecimalType.DEFAULT_DECIMAL128, DecimalType.DEFAULT_DECIMAL32);
            // Shrinking only the scale still loses information, so reject too.
            assertImplicitCastRejects(DecimalType.DEFAULT_DECIMAL64, DecimalType.DEFAULT_DECIMAL32);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeDateFamily() {
        enterStrictMode();
        try {
            // DATE -> DATETIME is widening.
            assertImplicitCastAccepts(DateType.DATE, DateType.DATETIME);
            // DATETIME -> DATE loses time-of-day, narrowing.
            assertImplicitCastRejects(DateType.DATETIME, DateType.DATE);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeStringFamilyLengthInsensitive() {
        // ScalarType.matchesType() returns true for any pair of string types,
        // so the rule short-circuits before reaching the string-family branch
        // in isImplicitCastSafe. Verify that strict mode never rejects a
        // same-family predicate such as CHAR = VARCHAR.
        enterStrictMode();
        try {
            BinaryPredicateOperator op = new BinaryPredicateOperator(BinaryType.EQ,
                    new ColumnRefOperator(1, new CharType(10), "c_char", true),
                    new ColumnRefOperator(2, new VarcharType(5), "c_varchar", true));
            ScalarOperator rewritten = new ImplicitCastRule().apply(op, null);
            // matchesType is true, so no cast is injected and the predicate
            // is returned unchanged.
            assertFalse(rewritten.getChild(0) instanceof CastOperator);
            assertFalse(rewritten.getChild(1) instanceof CastOperator);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeNullSourceAccepted() {
        // A NULL-typed source must always be treated as safe regardless of
        // target type (covers the isNull/isInvalid short-circuit).
        enterStrictMode();
        try {
            Function fn = new Function(new FunctionName("coverage_fn"),
                    new Type[] {IntegerType.BIGINT}, IntegerType.BIGINT, false);
            CallOperator op = new CallOperator("coverage_fn", IntegerType.BIGINT,
                    Lists.newArrayList(ConstantOperator.createNull(NullType.NULL)), fn);
            ScalarOperator rewritten = new ImplicitCastRule().apply(op, null);
            assertTrue(rewritten.getChild(0) instanceof CastOperator);
            assertEquals(IntegerType.BIGINT, rewritten.getChild(0).getType());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeComplexTypesPass() {
        enterStrictMode();
        try {
            // ARRAY<INT> -> ARRAY<BIGINT>: complex-type branch in the safety
            // check short-circuits so the outer cast is accepted.
            assertImplicitCastAccepts(new ArrayType(IntegerType.INT), new ArrayType(IntegerType.BIGINT));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeCrossFamily() {
        enterStrictMode();
        try {
            // All the cross-family shapes must be rejected.
            assertImplicitCastRejects(VarcharType.VARCHAR, IntegerType.INT);
            assertImplicitCastRejects(IntegerType.INT, VarcharType.VARCHAR);
            assertImplicitCastRejects(VarcharType.VARCHAR, DateType.DATETIME);
            assertImplicitCastRejects(DateType.DATETIME, VarcharType.VARCHAR);
            assertImplicitCastRejects(IntegerType.BIGINT, DateType.DATETIME);
            assertImplicitCastRejects(BooleanType.BOOLEAN, IntegerType.INT);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeInactiveWithoutContext() {
        // With no ConnectContext bound, the guard must be a no-op and the rule
        // should still produce a cast for any type mismatch.
        ConnectContext.remove();
        Function fn = new Function(new FunctionName("coverage_fn"), new Type[] {IntegerType.INT},
                IntegerType.INT, false);
        CallOperator op = new CallOperator("coverage_fn", IntegerType.INT,
                Lists.newArrayList(new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true)), fn);
        ScalarOperator rewritten = new ImplicitCastRule().apply(op, null);
        assertTrue(rewritten.getChild(0) instanceof CastOperator);
        assertEquals(IntegerType.INT, rewritten.getChild(0).getType());
    }

    @Test
    public void testForbidInvalidImplicitCastModeInPredicateLiteralFold() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.getSessionVariable().setSqlMode(SqlModeHelper.MODE_FORBID_INVALID_IMPLICIT_CAST);
        ctx.setThreadLocalInfo();
        try {
            // int_col IN ('1', '2') also takes a literal-fold fast path in
            // castForBetweenAndIn; strict mode must reject it.
            InPredicateOperator op = new InPredicateOperator(
                    new ColumnRefOperator(1, IntegerType.INT, "c_int", true),
                    ConstantOperator.createVarchar("1"),
                    ConstantOperator.createVarchar("2"));
            ImplicitCastRule rule = new ImplicitCastRule();
            assertThrows(SemanticException.class, () -> rule.apply(op, null));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeRejectsNarrowing() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.getSessionVariable().setSqlMode(SqlModeHelper.MODE_FORBID_INVALID_IMPLICIT_CAST);
        ctx.setThreadLocalInfo();
        try {
            // A function declared over INT, called with a BIGINT arg, would
            // need a narrowing implicit cast which Trino forbids.
            Function fn = new Function(new FunctionName("abs"), new Type[] {IntegerType.INT},
                    IntegerType.INT, false);
            CallOperator op = new CallOperator("abs", IntegerType.INT, Lists.newArrayList(
                    new ColumnRefOperator(1, IntegerType.BIGINT, "c_bigint", true)));
            new Expectations(op) {{
                    op.getFunction();
                    minTimes = 0;
                    result = fn;
                }};

            ImplicitCastRule rule = new ImplicitCastRule();
            assertThrows(SemanticException.class, () -> rule.apply(op, null));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testForbidInvalidImplicitCastModeDisabledByDefault() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setThreadLocalInfo();
        try {
            // Without the mode, the existing varchar<->int coercion still applies.
            BinaryPredicateOperator op = new BinaryPredicateOperator(BinaryType.EQ,
                    new ColumnRefOperator(1, IntegerType.INT, "c_int", true),
                    ConstantOperator.createVarchar("1"));
            ScalarOperator result = new ImplicitCastRule().apply(op, null);
            assertTrue(result.getChild(0) instanceof CastOperator || result.getChild(1) instanceof CastOperator
                    || result.getChild(1) instanceof ConstantOperator);
        } finally {
            ConnectContext.remove();
        }
    }
}