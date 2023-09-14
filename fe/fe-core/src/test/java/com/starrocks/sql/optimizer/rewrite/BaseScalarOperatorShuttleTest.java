// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.starrocks.catalog.Type.ARRAY_TINYINT;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;
import static com.starrocks.catalog.Type.TINYINT;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseScalarOperatorShuttleTest {

    private final BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle();

    private final BaseScalarOperatorShuttle shuttle2 = new BaseScalarOperatorShuttle() {
        @Override
        public Optional<ScalarOperator> preprocess(ScalarOperator scalarOperator) {
            return Optional.of(scalarOperator);
        }
    };

    @Test
    void visitArray() {
        ArrayOperator operator = new ArrayOperator(ARRAY_TINYINT, true, Lists.newArrayList(ConstantOperator.createInt(3)));
        {
            ScalarOperator newOperator = shuttle.visitArray(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitArray(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitCollectionElement() {
        ArrayOperator arrayOperator = new ArrayOperator(ARRAY_TINYINT, true, Lists.newArrayList(ConstantOperator.createInt(3)));
        CollectionElementOperator operator = new CollectionElementOperator(STRING, arrayOperator, ConstantOperator.createInt(0));
        {
            ScalarOperator newOperator = shuttle.visitCollectionElement(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCollectionElement(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitArraySlice() {
        ArrayOperator arrayOperator = new ArrayOperator(ARRAY_TINYINT, true,
                Lists.newArrayList(ConstantOperator.createInt(3), ConstantOperator.createInt(10)));
        ConstantOperator offset = ConstantOperator.createInt(0);
        ConstantOperator length = ConstantOperator.createInt(1);
        ArraySliceOperator operator = new ArraySliceOperator(TINYINT, Lists.newArrayList(arrayOperator, offset, length));
        {
            ScalarOperator newOperator = shuttle.visitArraySlice(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitArraySlice(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitBetweenPredicate() {
        BetweenPredicateOperator operator = new BetweenPredicateOperator(true,
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1), ConstantOperator.createInt(10));
        {
            ScalarOperator newOperator = shuttle.visitBetweenPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitBetweenPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }


    @Test
    void visitExistsPredicate() {
        ExistsPredicateOperator operator = new ExistsPredicateOperator(true, ImmutableList.of());
        {
            ScalarOperator newOperator = shuttle.visitExistsPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitExistsPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitInPredicate() {
        InPredicateOperator operator = new InPredicateOperator(true, ImmutableList.of());
        {
            ScalarOperator newOperator = shuttle.visitInPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitInPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitIsNullPredicate() {
        IsNullPredicateOperator operator = new IsNullPredicateOperator(true, new ColumnRefOperator(1, INT, "id", true));
        {
            ScalarOperator newOperator = shuttle.visitIsNullPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitIsNullPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitLikePredicateOperator() {
        LikePredicateOperator operator = new LikePredicateOperator(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.TRUE);
        {
            ScalarOperator newOperator = shuttle.visitLikePredicateOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitLikePredicateOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitCastOperator() {
        CastOperator operator = new CastOperator(INT, new ColumnRefOperator(1, INT, "id", true));
        {
            ScalarOperator newOperator = shuttle.visitCastOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCastOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitCaseWhenOperator() {
        CaseWhenOperator operator = new CaseWhenOperator(INT, null, null, ImmutableList.of());
        {
            ScalarOperator newOperator = shuttle.visitCaseWhenOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCaseWhenOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testCallOperator() {
        CallOperator operator = new CallOperator("count", INT, Lists.newArrayList());
        {
            ScalarOperator newOperator = shuttle.visitCall(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCall(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testBinaryOperator() {
        BinaryPredicateOperator operator = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        {
            ScalarOperator newOperator = shuttle.visitBinaryPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitBinaryPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testCompoundPredicate() {
        BinaryPredicateOperator binary1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        BinaryPredicateOperator binary2 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(2, INT, "id2", true), ConstantOperator.createInt(1));
        CompoundPredicateOperator compound =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, binary1, binary2);
        {
            ScalarOperator newOperator = shuttle.visitCompoundPredicate(compound, null);
            assertEquals(compound, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCompoundPredicate(compound, null);
            assertEquals(compound, newOperator);
        }
    }

    @Test
    void testLambdaFunctionOperator() {
        BinaryPredicateOperator binary1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        LambdaFunctionOperator lambda =
                new LambdaFunctionOperator(Lists.newArrayList(new ColumnRefOperator(1, INT, "id", true)), binary1, INT);
        {
            ScalarOperator newOperator = shuttle.visitLambdaFunctionOperator(lambda, null);
            assertEquals(lambda, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitLambdaFunctionOperator(lambda, null);
            assertEquals(lambda, newOperator);
        }
    }

    @Test
    void testCloneOperator() {
        BinaryPredicateOperator binary1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        CloneOperator clone = new CloneOperator(binary1);
        {
            ScalarOperator newOperator = shuttle.visitCloneOperator(clone, null);
            assertEquals(clone, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCloneOperator(clone, null);
            assertEquals(clone, newOperator);
        }
    }

    @Test
    void visitCaseWhenOperator_1() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        BinaryPredicateOperator whenOperator1 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(1));
        ConstantOperator constantOperator1 = ConstantOperator.createChar("1");
        BinaryPredicateOperator whenOperator2 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(2));
        ConstantOperator constantOperator2 = ConstantOperator.createChar("2");

        CaseWhenOperator operator =
                new CaseWhenOperator(Type.VARCHAR, null, ConstantOperator.createChar("others", Type.VARCHAR),
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));

        CaseWhenOperator otherOperator =
                new CaseWhenOperator(Type.VARCHAR, null, null,
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));

        BaseScalarOperatorShuttle testShuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
                return ConstantOperator.createChar("3");
            }
        };
        ScalarOperator newOperator = testShuttle.visitCaseWhenOperator(operator, null);
        assertNotEquals(operator, newOperator);
        newOperator = testShuttle.visitCaseWhenOperator(otherOperator, null);
        assertNotEquals(otherOperator, newOperator);

    }

    @ParameterizedTest(name = "{index}: {0}.")
    @MethodSource("inPredicateCases")
    void testNegateInPredicate(ScalarOperator operator, String expected) {
        NegateFilterShuttle shuttle1 = NegateFilterShuttle.getInstance();
        ScalarOperator reuslt = shuttle1.negateFilter(operator);
        assertEquals(expected, reuslt.toString());
    }

    private static Stream<Arguments> inPredicateCases() {
        List<Arguments> argumentsList = Lists.newArrayList();
        // not in constant values without null
        InPredicateOperator operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2)"));

        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2) OR 1: id IS NULL"));

        // not in constant values with null
        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2, null) OR 1: id NOT IN (1, 2, null) IS NULL"));

        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2, null) OR 1: id NOT IN (1, 2, null) IS NULL"));

        // not in values contains expr
        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                new CastOperator(INT, ConstantOperator.createChar("a"))));

        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2, cast(a as int(11))) " +
                "OR 1: id NOT IN (1, 2, cast(a as int(11))) IS NULL"));

        // in constant values without null
        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2)"));

        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2) OR 1: id IS NULL"));

        // in constant values with null
        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2, null) OR 1: id IN (1, 2, null) IS NULL"));

        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2, null) OR 1: id IN (1, 2, null) IS NULL"));

        // in values contains expr
        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                new CastOperator(INT, ConstantOperator.createChar("a"))));

        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2, cast(a as int(11))) " +
                "OR 1: id IN (1, 2, cast(a as int(11))) IS NULL"));
        return argumentsList.stream();
    }

}