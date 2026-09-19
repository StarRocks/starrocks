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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimplifiedPredicateRuleAbsorptionTest {
    private final SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

    @Test
    public void testAbsorptionLaw() {
        ColumnRefOperator a = new ColumnRefOperator(1, BooleanType.BOOLEAN, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, BooleanType.BOOLEAN, "b", true);

        // a AND (a OR b) -> a
        ScalarOperator input = CompoundPredicateOperator.and(a, CompoundPredicateOperator.or(a, b));
        assertEquals(a, rule.apply(input, null));

        // (a OR b) AND a -> a
        input = CompoundPredicateOperator.and(CompoundPredicateOperator.or(a, b), a);
        assertEquals(a, rule.apply(input, null));

        // a OR (a AND b) -> a
        input = CompoundPredicateOperator.or(a, CompoundPredicateOperator.and(a, b));
        assertEquals(a, rule.apply(input, null));

        // (a AND b) OR a -> a
        input = CompoundPredicateOperator.or(CompoundPredicateOperator.and(a, b), a);
        assertEquals(a, rule.apply(input, null));

        // a AND (a OR b) AND c -> a AND c
        ColumnRefOperator c = new ColumnRefOperator(3, BooleanType.BOOLEAN, "c", true);
        input = CompoundPredicateOperator.and(a, CompoundPredicateOperator.or(a, b), c);
        ScalarOperator result = rule.apply(input, null);
        assertEquals(CompoundPredicateOperator.and(a, c), result);

        // a AND (a OR rand()) -> a (rand() is discarded, safe to absorb)
        CallOperator rand = new CallOperator(FunctionSet.RAND, FloatType.DOUBLE, Lists.newArrayList());
        input = CompoundPredicateOperator.and(a, CompoundPredicateOperator.or(a, rand));
        assertEquals(a, rule.apply(input, null));

        // rand() AND (rand() OR b) should NOT be absorbed
        input = CompoundPredicateOperator.and(rand, CompoundPredicateOperator.or(rand, b));
        assertEquals(input, rule.apply(input, null));

        // Compound absorber: (a OR b) AND ((a OR b) OR c) -> a OR b
        ScalarOperator aOrB = CompoundPredicateOperator.or(a, b);
        input = CompoundPredicateOperator.and(aOrB, CompoundPredicateOperator.or(aOrB, c));
        assertEquals(aOrB, rule.apply(input, null));

        // Dual compound absorber: (a AND b) OR ((a AND b) AND c) -> a AND b
        ScalarOperator aAndB = CompoundPredicateOperator.and(a, b);
        input = CompoundPredicateOperator.or(aAndB, CompoundPredicateOperator.and(aAndB, c));
        assertEquals(aAndB, rule.apply(input, null));
    }
}
