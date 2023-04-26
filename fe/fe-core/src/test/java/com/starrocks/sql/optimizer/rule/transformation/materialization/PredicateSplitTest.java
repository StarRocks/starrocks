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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
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
                BinaryPredicateOperator.BinaryType.EQ, columnRef1, columnRef2);
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryPredicateOperator.BinaryType.GE, columnRef1, ConstantOperator.createInt(1));

        List<ScalarOperator> arguments = Lists.newArrayList();
        arguments.add(columnRef1);
        arguments.add(columnRef2);
        CallOperator callOperator = new CallOperator(FunctionSet.SUM, Type.INT, arguments);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryPredicateOperator.BinaryType.GE, callOperator, ConstantOperator.createInt(1));
        ScalarOperator andPredicate = Utils.compoundAnd(binaryPredicate, binaryPredicate2, binaryPredicate3);
        PredicateSplit result = PredicateSplit.splitPredicate(andPredicate);
        Assert.assertEquals(binaryPredicate, result.getEqualPredicates());
        Assert.assertEquals(binaryPredicate2, result.getRangePredicates());
        Assert.assertEquals(binaryPredicate3, result.getResidualPredicates());
    }
}
