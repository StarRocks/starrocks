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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PredicateSplitTest {
    @Test
    public void testSplitPredicate() {
        ScalarOperator predicate = null;
        PredicateSplit split = PredicateSplit.splitPredicate(predicate);
        Assert.assertNotNull(split);
        Assert.assertNull(split.getEqualPredicates());
        Assert.assertNull(split.getRangePredicates());
        Assert.assertNull(split.getResidualPredicates());

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", Type.INT, false);
        ColumnRefOperator columnRef2 = columnRefFactory.create("col2", Type.INT, false);
        BinaryPredicateOperator binaryPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef2);
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef1, ConstantOperator.createInt(1));

        List<ScalarOperator> arguments = Lists.newArrayList();
        arguments.add(columnRef1);
        arguments.add(columnRef2);
        CallOperator callOperator = new CallOperator(FunctionSet.SUM, Type.INT, arguments);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryType.GE, callOperator, ConstantOperator.createInt(1));
        ScalarOperator andPredicate = Utils.compoundAnd(binaryPredicate, binaryPredicate2, binaryPredicate3);
        PredicateSplit result = PredicateSplit.splitPredicate(andPredicate);
        Assert.assertEquals("1: col1 = 2: col2", result.getEqualPredicates().toString());
        Assert.assertEquals("1: col1 >= 1 AND sum(1: col1, 2: col2) >= 1", result.getRangePredicates().toString());
        Assert.assertNull(result.getResidualPredicates());
    }

    @Test
    public void testSplitPredicateWithMultiRange() {
        ColumnRefOperator a = new ColumnRefOperator(0, Type.INT, "a", false);
        ColumnRefOperator b = new ColumnRefOperator(1, Type.INT, "b", false);
        ColumnRefOperator c = new ColumnRefOperator(2, Type.INT, "c", false);
        ColumnRefOperator d = new ColumnRefOperator(3, Type.INT, "d", false);

        ScalarOperator rangePredicate = CompoundPredicateOperator.or(
                CompoundPredicateOperator.and(
                    BinaryPredicateOperator.ge(a, ConstantOperator.createInt(0)),
                    BinaryPredicateOperator.lt(a, ConstantOperator.createInt(3))
                ),
                CompoundPredicateOperator.and(
                    BinaryPredicateOperator.ge(a, ConstantOperator.createInt(4)),
                    BinaryPredicateOperator.lt(a, ConstantOperator.createInt(7))
                )
        );
        BinaryPredicateOperator equalPredicate = BinaryPredicateOperator.eq(c, d);
        InPredicateOperator inPredicate = new InPredicateOperator(
                b,
                ConstantOperator.createInt(0), ConstantOperator.createInt(1));
        ScalarOperator otherPredicate = CompoundPredicateOperator.and(
                BinaryPredicateOperator.ne(b, ConstantOperator.createInt(1)),
                inPredicate
        );

        {
            {
                ScalarOperator predicate = CompoundPredicateOperator.not(
                        CompoundPredicateOperator.or(
                                CompoundPredicateOperator.and(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(0)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(3))
                                ),
                                CompoundPredicateOperator.and(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(4)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(7))
                                )
                        ));
                ScalarOperator result = CompoundPredicateOperator.and(
                        CompoundPredicateOperator.or(
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(a, ConstantOperator.createInt(0)),
                                        BinaryPredicateOperator.gt(a, ConstantOperator.createInt(0))),
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(b, ConstantOperator.createInt(3)),
                                        BinaryPredicateOperator.gt(b, ConstantOperator.createInt(3)))
                        ),
                        CompoundPredicateOperator.or(
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(a, ConstantOperator.createInt(4)),
                                        BinaryPredicateOperator.gt(a, ConstantOperator.createInt(4))),
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(b, ConstantOperator.createInt(7)),
                                        BinaryPredicateOperator.gt(b, ConstantOperator.createInt(7)))
                        )
                );
                PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
                Assert.assertEquals(result, predicateSplit.getRangePredicates());
            }
        }

        {
            {
                ScalarOperator predicate = CompoundPredicateOperator.not(
                        CompoundPredicateOperator.and(
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(0)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(3))
                                ),
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(4)),
                                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(7))
                                )
                        ));
                ScalarOperator result = CompoundPredicateOperator.or(
                        CompoundPredicateOperator.and(
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(a, ConstantOperator.createInt(0)),
                                        BinaryPredicateOperator.gt(a, ConstantOperator.createInt(0))),
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(b, ConstantOperator.createInt(3)),
                                        BinaryPredicateOperator.gt(b, ConstantOperator.createInt(3)))
                        ),
                        CompoundPredicateOperator.and(
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(a, ConstantOperator.createInt(4)),
                                        BinaryPredicateOperator.gt(a, ConstantOperator.createInt(4))),
                                CompoundPredicateOperator.or(
                                        BinaryPredicateOperator.lt(b, ConstantOperator.createInt(7)),
                                        BinaryPredicateOperator.gt(b, ConstantOperator.createInt(7)))
                        )
                );
                PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
                Assert.assertEquals(result, predicateSplit.getRangePredicates());
            }
        }

        {
            ScalarOperator predicate = CompoundPredicateOperator.and(rangePredicate, equalPredicate, otherPredicate);

            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assert.assertEquals(equalPredicate, predicateSplit.getEqualPredicates());
            ScalarOperator expectRange = CompoundPredicateOperator.and(
                    rangePredicate,
                    CompoundPredicateOperator.or(
                        BinaryPredicateOperator.lt(b, ConstantOperator.createInt(1)),
                        BinaryPredicateOperator.gt(b, ConstantOperator.createInt(1)))
            );
            Assert.assertEquals(expectRange, predicateSplit.getRangePredicates());
            Assert.assertEquals(inPredicate, predicateSplit.getResidualPredicates());
        }
        {
            ScalarOperator residual = CompoundPredicateOperator.or(equalPredicate, otherPredicate);
            ScalarOperator predicate = CompoundPredicateOperator.and(residual, rangePredicate);
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assert.assertNull(predicateSplit.getEqualPredicates());
            Assert.assertEquals(rangePredicate, predicateSplit.getRangePredicates());
            Assert.assertEquals(residual, predicateSplit.getResidualPredicates());
        }

        {
            ScalarOperator predicate = CompoundPredicateOperator.and(
                    CompoundPredicateOperator.or(
                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(0)),
                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(3))
                    ),
                    CompoundPredicateOperator.or(
                        BinaryPredicateOperator.eq(a, ConstantOperator.createInt(4)),
                        BinaryPredicateOperator.eq(b, ConstantOperator.createInt(7))
                    )
            );
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assert.assertEquals(predicate, predicateSplit.getRangePredicates());
        }

        {
            IsNullPredicateOperator predicate = new IsNullPredicateOperator(a);
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assert.assertEquals(predicate, predicateSplit.getResidualPredicates());
        }

        {
            ScalarOperator predicate = CompoundPredicateOperator.or(
                    BinaryPredicateOperator.ge(a, ConstantOperator.createInt(2000)),
                    new IsNullPredicateOperator(a)
            );
            PredicateSplit predicateSplit = PredicateSplit.splitPredicate(predicate);
            Assert.assertEquals(predicate, predicateSplit.getResidualPredicates());
        }
    }
}
