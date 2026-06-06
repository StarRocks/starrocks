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
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionalTypeCheckerTest {

    private static OptExpression wrapInProject(ColumnRefOperator output, ScalarOperator scalar) {
        Map<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(output, scalar);
        LogicalProjectOperator project = new LogicalProjectOperator(map);
        OptExpression input = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        input.setLogicalProperty(new LogicalProperty(new ColumnRefSet()));
        OptExpression root = OptExpression.create(project, input);
        root.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(output)));
        return root;
    }

    @Test
    void testCoalesceArgTypeMismatchIsRejected() {
        // coalesce(DATETIME_col, VARCHAR_col) declared as VARCHAR with the
        // DATETIME child *not* wrapped in a CAST.  This is the malformed
        // shape the planner used to emit before the JOIN-USING fix.
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        ColumnRefOperator s = new ColumnRefOperator(2, TypeFactory.createVarcharType(255), "s", true);
        CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, VarcharType.VARCHAR,
                Lists.newArrayList(dt, s));
        ColumnRefOperator out = new ColumnRefOperator(3, VarcharType.VARCHAR, "out", true);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> ConditionalTypeChecker.getInstance().validate(wrapInProject(out, coalesce), null));
        assertTrue(ex.getMessage().contains("Conditional expression type check failed"),
                "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testCoalesceWithExplicitCastIsAccepted() {
        // coalesce(CAST(DATETIME_col AS VARCHAR), VARCHAR_col) -- well-formed.
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        ColumnRefOperator s = new ColumnRefOperator(2, TypeFactory.createVarcharType(255), "s", true);
        CastOperator dtAsString = new CastOperator(VarcharType.VARCHAR, dt, true);
        CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, VarcharType.VARCHAR,
                Lists.newArrayList(dtAsString, s));
        ColumnRefOperator out = new ColumnRefOperator(3, VarcharType.VARCHAR, "out", true);

        assertDoesNotThrow(() ->
                ConditionalTypeChecker.getInstance().validate(wrapInProject(out, coalesce), null));
    }

    @Test
    void testIfThenElseTypeMismatchIsRejected() {
        ColumnRefOperator cond = new ColumnRefOperator(1, BooleanType.BOOLEAN, "c", true);
        ColumnRefOperator a = new ColumnRefOperator(2, IntegerType.BIGINT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(3, TypeFactory.createVarcharType(255), "b", true);
        // Result type BIGINT, but the else branch is VARCHAR -- malformed.
        CallOperator ifCall = new CallOperator(FunctionSet.IF, IntegerType.BIGINT,
                Lists.newArrayList(cond, a, b));
        ColumnRefOperator out = new ColumnRefOperator(4, IntegerType.BIGINT, "out", true);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> ConditionalTypeChecker.getInstance().validate(wrapInProject(out, ifCall), null));
        assertTrue(ex.getMessage().contains("Conditional expression type check failed"),
                "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testCaseWhenThenTypeMismatchIsRejected() {
        ColumnRefOperator c = new ColumnRefOperator(1, BooleanType.BOOLEAN, "c", true);
        ColumnRefOperator t = new ColumnRefOperator(2, IntegerType.BIGINT, "t", true);
        // ELSE clause is VARCHAR, while the result type is BIGINT -- malformed.
        ColumnRefOperator e = new ColumnRefOperator(3, TypeFactory.createVarcharType(255), "e", true);
        CaseWhenOperator caseWhen = new CaseWhenOperator(IntegerType.BIGINT,
                null, e, Lists.newArrayList(c, t));
        ColumnRefOperator out = new ColumnRefOperator(4, IntegerType.BIGINT, "out", true);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> ConditionalTypeChecker.getInstance().validate(wrapInProject(out, caseWhen), null));
        assertTrue(ex.getMessage().contains("Conditional expression type check failed"),
                "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testWellFormedCaseWhenIsAccepted() {
        ColumnRefOperator c = new ColumnRefOperator(1, BooleanType.BOOLEAN, "c", true);
        ColumnRefOperator t = new ColumnRefOperator(2, IntegerType.BIGINT, "t", true);
        ColumnRefOperator e = new ColumnRefOperator(3, IntegerType.BIGINT, "e", true);
        CaseWhenOperator caseWhen = new CaseWhenOperator(IntegerType.BIGINT,
                null, e, Lists.newArrayList(c, t));
        ColumnRefOperator out = new ColumnRefOperator(4, IntegerType.BIGINT, "out", true);

        assertDoesNotThrow(() ->
                ConditionalTypeChecker.getInstance().validate(wrapInProject(out, caseWhen), null));
    }

    @Test
    void testStringTypesOfDifferentLengthsAreAccepted() {
        // VARCHAR(255) and VARCHAR(100) should be considered compatible by
        // matchesType, so a coalesce mixing them must not be rejected.
        ColumnRefOperator a = new ColumnRefOperator(1, TypeFactory.createVarcharType(255), "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, TypeFactory.createVarcharType(100), "b", true);
        Type result = TypeFactory.createVarcharType(255);
        CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, result, Lists.newArrayList(a, b));
        ColumnRefOperator out = new ColumnRefOperator(3, result, "out", true);

        assertDoesNotThrow(() ->
                ConditionalTypeChecker.getInstance().validate(wrapInProject(out, coalesce), null));
    }
}
