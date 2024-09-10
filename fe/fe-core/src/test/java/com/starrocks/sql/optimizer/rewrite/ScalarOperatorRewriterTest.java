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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.MvNormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Collectors;

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
        assertEquals(OperatorType.CALL, result.getChild(0).getChild(1).getOpType());

        assertEquals(Type.VARCHAR.getPrimitiveType(), result.getChild(0).getChild(0).getType().getPrimitiveType());
        assertEquals(Type.VARCHAR.getPrimitiveType(), result.getChild(0).getChild(1).getType().getPrimitiveType());

        assertEquals(OperatorType.COMPOUND, result.getChild(1).getOpType());
        assertEquals(OperatorType.BINARY, result.getChild(1).getChild(0).getOpType());
        assertEquals(OperatorType.VARIABLE, result.getChild(1).getChild(0).getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, result.getChild(1).getChild(0).getChild(1).getOpType());

        assertEquals(OperatorType.CALL, result.getChild(1).getChild(1).getOpType());
        assertEquals(Type.BOOLEAN, result.getChild(1).getChild(1).getType());
    }

    @Test
    public void testRewrite3() {
        ConstantOperator constFalse = ConstantOperator.FALSE;
        assertEquals(ConstantOperator.TRUE, NegateFilterShuttle.getInstance().negateFilter(constFalse));
        constFalse = ConstantOperator.TRUE;
        assertEquals(ConstantOperator.FALSE, NegateFilterShuttle.getInstance().negateFilter(constFalse));
        constFalse = ConstantOperator.NULL;
        assertEquals(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, constFalse),
                NegateFilterShuttle.getInstance().negateFilter(constFalse));
    }

    @Test
    public void testNormalizePredicate() {
        // b > a => a < b
        {
            BinaryPredicateOperator op = new BinaryPredicateOperator(BinaryType.GT,
                    new ColumnRefOperator(0, Type.VARCHAR, "b", true),
                    new ColumnRefOperator(1, Type.BIGINT, "a", true)
            );

            ScalarOperatorRewriter operatorRewriter = new ScalarOperatorRewriter();
            ScalarOperator result = operatorRewriter.rewrite(op, Lists.newArrayList(new MvNormalizePredicateRule()));

            Assert.assertEquals("1: a < 0: b", result.toString());
        }

        // b:101 > b:2 => b:2 < b:101
        {
            BinaryPredicateOperator op = new BinaryPredicateOperator(BinaryType.GT,
                    new ColumnRefOperator(101, Type.VARCHAR, "b", true),
                    new ColumnRefOperator(2, Type.BIGINT, "b", true)
            );

            ScalarOperatorRewriter operatorRewriter = new ScalarOperatorRewriter();
            ScalarOperator result = operatorRewriter.rewrite(op, Lists.newArrayList(new MvNormalizePredicateRule()));

            Assert.assertEquals("2: b < 101: b", result.toString());
        }
    }

    @Test
    public void testNormalizeIsNull() {
        ColumnRefOperator column1 = new ColumnRefOperator(0, Type.INT, "test0", false);
        IsNullPredicateOperator isnotNull = new IsNullPredicateOperator(true, column1);
        ScalarOperator rewritten = new ScalarOperatorRewriter()
                .rewrite(isnotNull, ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES);
        Assert.assertEquals(ConstantOperator.TRUE, rewritten);

        ScalarOperator rewritten2 = new ScalarOperatorRewriter()
                .rewrite(isnotNull, ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
        Assert.assertEquals(ConstantOperator.TRUE, rewritten2);
    }

    @Test
    public void testRangeExtract() {
        Supplier<ScalarOperator> predicate1Maker = () -> {
            ColumnRefOperator col1 = new ColumnRefOperator(1, Type.DATE, "dt", false);
            CallOperator call = new CallOperator(FunctionSet.DATE_TRUNC, Type.DATE,
                    Arrays.asList(ConstantOperator.createVarchar("YEAR"), col1));
            return new BinaryPredicateOperator(BinaryType.EQ, call, ConstantOperator.createDate(
                    LocalDateTime.of(2024, 1, 1, 0, 0, 0)));
        };

        Supplier<ScalarOperator> predicate2Maker = () -> {
            ColumnRefOperator col2 = new ColumnRefOperator(2, Type.VARCHAR, "mode", false);
            return new BinaryPredicateOperator(BinaryType.EQ, col2, ConstantOperator.createVarchar("Buzz"));
        };

        ScalarOperator predicate1 = Utils.compoundAnd(predicate1Maker.get(), predicate2Maker.get());
        ScalarOperator predicate2 = Utils.compoundAnd(predicate1Maker.get(), predicate2Maker.get());
        ScalarOperator predicates = Utils.compoundAnd(predicate1, predicate2);

        ScalarRangePredicateExtractor rangeExtractor = new ScalarRangePredicateExtractor();
        ScalarOperator result = rangeExtractor.rewriteOnlyColumn(Utils.compoundAnd(Utils.extractConjuncts(predicates)
                .stream().map(rangeExtractor::rewriteOnlyColumn).collect(Collectors.toList())));
        Preconditions.checkState(result != null);
        String expect = "date_trunc(YEAR, 1: dt) = 2024-01-01 AND 2: mode = Buzz";
        String actual = result.toString();
        Assert.assertEquals(actual, expect, actual);
    }
}
