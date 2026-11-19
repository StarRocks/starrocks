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

package com.starrocks.sql.optimizer.validate;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConstInPredicateCheckerTest {

    private static final ColumnRefOperator VALUE = new ColumnRefOperator(1, IntegerType.TINYINT, "v", true);
    private static final ColumnRefOperator AUTO_FILL = new ColumnRefOperator(14, IntegerType.TINYINT,
            "auto_fill_col", false);

    private static OptExpression filter(ScalarOperator predicate) {
        PhysicalFilterOperator filter = new PhysicalFilterOperator(predicate, Operator.DEFAULT_LIMIT, null);
        return OptExpression.create(filter, new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES)));
    }

    @Test
    void testColumnRefInValueListIsRejected() {
        // `v IN (0, auto_fill_col)`: the shape PullUpScanPredicateRule produced from `v IN (0, 1)`.
        InPredicateOperator in = new InPredicateOperator(false, VALUE,
                ConstantOperator.createTinyInt((byte) 0), AUTO_FILL);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> ConstInPredicateChecker.getInstance().validate(filter(in), null));
        assertTrue(ex.getMessage().contains("Const IN-predicate check failed"),
                "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testColumnRefUnderCastInValueListIsRejected() {
        InPredicateOperator in = new InPredicateOperator(true, VALUE,
                ConstantOperator.createTinyInt((byte) 0), new CastOperator(IntegerType.TINYINT, AUTO_FILL));

        assertThrows(StarRocksPlannerException.class,
                () -> ConstInPredicateChecker.getInstance().validate(filter(in), null));
    }

    @Test
    void testNestedInProjectionIsRejected() {
        InPredicateOperator in = new InPredicateOperator(false, VALUE,
                ConstantOperator.createTinyInt((byte) 0), AUTO_FILL);
        ColumnRefOperator out = new ColumnRefOperator(20, BooleanType.BOOLEAN, "out", true);
        PhysicalProjectOperator project = new PhysicalProjectOperator(ImmutableMap.of(out, in), ImmutableMap.of());
        OptExpression root = OptExpression.create(project,
                new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES)));

        assertThrows(StarRocksPlannerException.class,
                () -> ConstInPredicateChecker.getInstance().validate(root, null));
    }

    @Test
    void testConstantValueListIsAccepted() {
        InPredicateOperator in = new InPredicateOperator(false, VALUE,
                ConstantOperator.createTinyInt((byte) 0), ConstantOperator.createTinyInt((byte) 1),
                ConstantOperator.createNull(IntegerType.TINYINT));

        assertDoesNotThrow(() -> ConstInPredicateChecker.getInstance().validate(filter(in), null));
    }
}
