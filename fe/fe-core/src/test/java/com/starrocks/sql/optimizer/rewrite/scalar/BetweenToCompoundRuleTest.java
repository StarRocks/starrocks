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

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BetweenToCompoundRuleTest {

    @Test
    public void testApplyBetween() {
        NormalizePredicateRule rule = new NormalizePredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        BetweenPredicateOperator bpo = new BetweenPredicateOperator(false,
                new ColumnRefOperator(0, Type.INT, "test", true),
                ConstantOperator.createInt(1),
                new ColumnRefOperator(1, Type.INT, "value1", true));

        ScalarOperator result = rule.apply(bpo, context);

        assertEquals(OperatorType.COMPOUND, result.getOpType());
        assertEquals(Type.BOOLEAN, result.getType());

        assertTrue(result instanceof CompoundPredicateOperator);
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) result).getCompoundType());

        assertEquals(OperatorType.BINARY, result.getChild(0).getOpType());
        assertEquals(BinaryType.GE,
                ((BinaryPredicateOperator) result.getChild(0)).getBinaryType());
        assertEquals(OperatorType.VARIABLE, result.getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getChild(1).getOpType());

        assertEquals(BinaryType.LE,
                ((BinaryPredicateOperator) result.getChild(1)).getBinaryType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(0).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(1).getOpType());
    }

    @Test
    public void testApplyNotBetween() {
        NormalizePredicateRule rule = new NormalizePredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        BetweenPredicateOperator bpo = new BetweenPredicateOperator(true,
                new ColumnRefOperator(0, Type.INT, "test", true),
                ConstantOperator.createInt(1),
                new ColumnRefOperator(1, Type.INT, "value1", true));

        ScalarOperator result = rule.apply(bpo, context);

        assertEquals(OperatorType.COMPOUND, result.getOpType());
        assertEquals(Type.BOOLEAN, result.getType());

        assertTrue(result instanceof CompoundPredicateOperator);
        assertEquals(CompoundPredicateOperator.CompoundType.OR,
                ((CompoundPredicateOperator) result).getCompoundType());

        assertEquals(OperatorType.BINARY, result.getChild(0).getOpType());
        assertEquals(BinaryType.LT,
                ((BinaryPredicateOperator) result.getChild(0)).getBinaryType());
        assertEquals(OperatorType.VARIABLE, result.getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getChild(1).getOpType());

        assertEquals(BinaryType.GT,
                ((BinaryPredicateOperator) result.getChild(1)).getBinaryType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(0).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(1).getOpType());
    }

}
