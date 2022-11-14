// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.Test;

import static com.starrocks.catalog.Type.ARRAY_TINYINT;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseScalarOperatorShuttleTest {

    private final BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle();


    @Test
    void visitArray() {
        ArrayOperator operator = new ArrayOperator(ARRAY_TINYINT, true, null);
        ScalarOperator newOperator = shuttle.visitArray(operator, null);
        assertEquals(operator, newOperator);

    }

    @Test
    void visitCollectionElement() {
        CollectionElementOperator operator = new CollectionElementOperator(STRING, null, null);
        ScalarOperator newOperator = shuttle.visitCollectionElement(operator, null);
        assertEquals(operator, newOperator);
    }

    @Test
    void visitArraySlice() {
        ArraySliceOperator operator = new ArraySliceOperator(STRING, null);
        ScalarOperator newOperator = shuttle.visitArraySlice(operator, null);
        assertEquals(operator, newOperator);
    }

    @Test
    void visitBetweenPredicate() {
        BetweenPredicateOperator operator = new BetweenPredicateOperator(true,
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.TRUE, ConstantOperator.TRUE);
        ScalarOperator newOperator = shuttle.visitBetweenPredicate(operator, null);
        assertEquals(operator, newOperator);
    }


    @Test
    void visitExistsPredicate() {
        ExistsPredicateOperator operator = new ExistsPredicateOperator(true, ImmutableList.of());
        ScalarOperator newOperator = shuttle.visitExistsPredicate(operator, null);
        assertEquals(operator, newOperator);
    }

    @Test
    void visitInPredicate() {
        InPredicateOperator operator = new InPredicateOperator(true, ImmutableList.of());
        ScalarOperator newOperator = shuttle.visitInPredicate(operator, null);
        assertEquals(operator, newOperator);
    }

    @Test
    void visitIsNullPredicate() {
        IsNullPredicateOperator operator = new IsNullPredicateOperator(true, null);
        ScalarOperator newOperator = shuttle.visitIsNullPredicate(operator, null);
        assertEquals(operator, newOperator);
    }

    @Test
    void visitLikePredicateOperator() {
        LikePredicateOperator operator = new LikePredicateOperator(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.TRUE);
        ScalarOperator newOperator = shuttle.visitLikePredicateOperator(operator, null);
        assertEquals(operator, newOperator);
    }

    @Test
    void visitCastOperator() {
        CastOperator operator = new CastOperator(INT, new ColumnRefOperator(1, INT, "id", true));
        ScalarOperator newOperator = shuttle.visitCastOperator(operator, null);
        assertEquals(operator, newOperator);

    }

    @Test
    void visitCaseWhenOperator() {
        CaseWhenOperator operator = new CaseWhenOperator(INT, null, null, ImmutableList.of());
        ScalarOperator newOperator = shuttle.visitCaseWhenOperator(operator, null);
        assertEquals(operator, newOperator);
    }
}