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
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NormalizePredicateRuleTest {
    @Test
    public void testRule() {
        NormalizePredicateRule rule = new NormalizePredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        BinaryPredicateOperator bpo = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, Type.INT, "test", true),
                ConstantOperator.createInt(1));

        ScalarOperator result = rule.apply(bpo, context);

        assertEquals(bpo, result);
        assertEquals(OperatorType.VARIABLE, result.getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(1).getOpType());
    }

    @Test
    public void testRule1() {
        NormalizePredicateRule rule = new NormalizePredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        BinaryPredicateOperator bpo = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createInt(1),
                new ColumnRefOperator(1, Type.INT, "test", true));

        ScalarOperator result = rule.apply(bpo, context);

        assertNotEquals(bpo, result);
        assertEquals(OperatorType.VARIABLE, result.getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(1).getOpType());
    }

    @Test
    public void testRule2() {
        NormalizePredicateRule rule = new NormalizePredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        BinaryPredicateOperator bpo = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2));

        ScalarOperator result = rule.apply(bpo, context);

        assertEquals(bpo, result);
    }

    @Test
    public void testInPredicate() {
        NormalizePredicateRule rule = new NormalizePredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        InPredicateOperator inOp = new InPredicateOperator(
                ConstantOperator.createInt(1),
                new ColumnRefOperator(0, Type.INT, "col1", true)
        );

        ScalarOperator result = rule.apply(inOp, context);
        BinaryPredicateOperator eqOp = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createInt(1),
                new ColumnRefOperator(0, Type.INT, "col1", true)
        );

        assertEquals(eqOp, result);
    }
}