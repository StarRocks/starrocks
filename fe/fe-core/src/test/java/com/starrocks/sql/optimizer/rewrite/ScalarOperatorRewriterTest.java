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

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScalarOperatorRewriterTest {

    @Test
    public void testRewrite() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new BetweenPredicateOperator(false, new ColumnRefOperator(3, Type.INT, "test3", true),
                        new ColumnRefOperator(4, Type.INT, "test4", true),
                        new ColumnRefOperator(5, Type.INT, "test5", true)),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        new BinaryPredicateOperator(BinaryType.EQ,
                                ConstantOperator.createInt(1),
                                new ColumnRefOperator(1, Type.INT, "test1", true)),
                        new BinaryPredicateOperator(BinaryType.EQ,
                                ConstantOperator.createInt(1),
                                new ColumnRefOperator(2, Type.INT, "test2", true))
                ));

        ScalarOperatorRewriter operatorRewriter = new ScalarOperatorRewriter();
        ScalarOperator result = operatorRewriter
                .rewrite(root, Lists.newArrayList(new NormalizePredicateRule(), new SimplifiedPredicateRule()));

        assertEquals(root, result);
        assertEquals(OperatorType.COMPOUND, result.getChild(0).getOpType());
        assertTrue(result.getChild(0) instanceof CompoundPredicateOperator);
        assertEquals(CompoundPredicateOperator.CompoundType.AND,
                ((CompoundPredicateOperator) result.getChild(0)).getCompoundType());

        assertEquals(OperatorType.BINARY, result.getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.BINARY, result.getChild(0).getChild(1).getOpType());

        assertEquals(OperatorType.COMPOUND, result.getChild(1).getOpType());
        assertEquals(OperatorType.BINARY, result.getChild(1).getChild(0).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(1).getChild(0).getChild(1).getOpType());

        assertEquals(OperatorType.BINARY, result.getChild(1).getChild(1).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(1).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(1).getChild(1).getChild(1).getOpType());
    }

    @Test
    public void testRewrite2() {
        ScalarOperator root = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryType.EQ,
                        ConstantOperator.createInt(1),
                        new ColumnRefOperator(0, Type.VARCHAR, "test0", true)),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        new BinaryPredicateOperator(BinaryType.EQ,
                                ConstantOperator.createInt(1),
                                new ColumnRefOperator(1, Type.BIGINT, "test1", true)),
                        new CastOperator(Type.BOOLEAN, ConstantOperator.createNull(Type.BIGINT))));

        ScalarOperatorRewriter operatorRewriter = new ScalarOperatorRewriter();
        ScalarOperator result = operatorRewriter.rewrite(root,
                Lists.newArrayList(new NormalizePredicateRule(), new ImplicitCastRule(), new ReduceCastRule()));

        assertEquals(root, result);
        assertEquals(OperatorType.BINARY, result.getChild(0).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getChild(1).getOpType());

        assertEquals(Type.VARCHAR, result.getChild(0).getChild(0).getType());
        assertEquals(Type.VARCHAR, result.getChild(0).getChild(1).getType());

        assertEquals(OperatorType.COMPOUND, result.getChild(1).getOpType());
        assertEquals(OperatorType.BINARY, result.getChild(1).getChild(0).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(1).getChild(0).getChild(1).getOpType());

        assertEquals(OperatorType.CALL, result.getChild(1).getChild(1).getOpType());
        assertEquals(Type.BOOLEAN, result.getChild(1).getChild(1).getType());
    }
}
