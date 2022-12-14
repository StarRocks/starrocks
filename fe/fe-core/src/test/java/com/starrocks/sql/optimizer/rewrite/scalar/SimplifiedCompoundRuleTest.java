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

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimplifiedCompoundRuleTest {
    @Test
    public void applyAnd1() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true),
                ConstantOperator.createBoolean(false));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(ConstantOperator.createBoolean(false), result);
    }

    @Test
    public void applyAnd2() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true),
                ConstantOperator.createBoolean(true));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(new ColumnRefOperator(1, Type.BOOLEAN, "name", true), result);
    }

    @Test
    public void applyAnd3() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true),
                ConstantOperator.createNull(Type.BOOLEAN));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(root, result);
    }

    @Test
    public void applyOr1() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true),
                ConstantOperator.createBoolean(false));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(new ColumnRefOperator(1, Type.BOOLEAN, "name", true), result);
    }

    @Test
    public void applyOr2() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true),
                ConstantOperator.createBoolean(true));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(ConstantOperator.createBoolean(true), result);
    }

    @Test
    public void applyOr3() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true),
                ConstantOperator.createNull(Type.VARCHAR));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(root, result);
    }

    @Test
    public void applyNot1() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                new ColumnRefOperator(1, Type.BOOLEAN, "name", true));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(root, result);
    }

    @Test
    public void applyNot2() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                ConstantOperator.createBoolean(true));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(ConstantOperator.createBoolean(false), result);
    }

    @Test
    public void applyNot3() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                ConstantOperator.createNull(Type.BOOLEAN));

        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator result = rule.apply(root, null);

        assertEquals(ConstantOperator.createNull(Type.BOOLEAN), result);
    }

}