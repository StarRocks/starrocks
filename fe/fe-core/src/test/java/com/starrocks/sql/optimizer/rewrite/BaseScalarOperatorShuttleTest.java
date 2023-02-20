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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
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
import static org.junit.Assert.assertNotEquals;
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
}