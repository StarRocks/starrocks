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
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractCommonPredicateRuleTest {

    @Test
    public void testCompoundPredicate() {
        ExtractCommonPredicateRule rule = new ExtractCommonPredicateRule();
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

        CompoundPredicateOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                new ColumnRefOperator(1, Type.INT, "a", true),
                                new ColumnRefOperator(2, Type.INT, "b", true)),
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                new ColumnRefOperator(3, Type.INT, "c", true),
                                ConstantOperator.createInt(1))),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                new ColumnRefOperator(1, Type.INT, "a", true),
                                new ColumnRefOperator(2, Type.INT, "b", true)),
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                new ColumnRefOperator(4, Type.INT, "d", true),
                                ConstantOperator.createInt(2))));

        ScalarOperator result = rule.apply(or, context);

        ScalarOperator expect = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                        new ColumnRefOperator(1, Type.INT, "a", true),
                        new ColumnRefOperator(2, Type.INT, "b", true)),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                new ColumnRefOperator(3, Type.INT, "c", true),
                                ConstantOperator.createInt(1)),
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                new ColumnRefOperator(4, Type.INT, "d", true),
                                ConstantOperator.createInt(2)))
        );
        assertEquals(expect, result);

        or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                        ConstantOperator.createInt(1), new ColumnRefOperator(1, Type.BIGINT, "a", true)),
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                        ConstantOperator.createInt(1), new ColumnRefOperator(1, Type.BIGINT, "a", true)));
        result = rule.apply(or, context);
        expect = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                ConstantOperator.createInt(1),
                new ColumnRefOperator(1, Type.BIGINT, "a", true));
        assertEquals(expect, result);
    }
}