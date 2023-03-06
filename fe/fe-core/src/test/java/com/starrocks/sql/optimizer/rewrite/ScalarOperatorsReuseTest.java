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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.ScalarOperatorsReuse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ScalarOperatorsReuseTest {
    private static final Logger LOG = LogManager.getLogger(ScalarOperatorsReuseTest.class);

    private ColumnRefFactory columnRefFactory;

    @Before
    public void init() {
        columnRefFactory = new ColumnRefFactory();
    }

    @Test
    public void testTwoAdd() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(2)));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, ConstantOperator.createInt(3)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(add1, add2);

        List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);

        add2.setChild(0, columnRefFactory.getColumnRef(2));
        List<ScalarOperator> exceptResult = Lists.newArrayList(
                columnRefFactory.getColumnRef(2),
                add2);

        assertEquals(exceptResult, newOperators);
    }

    @Test
    public void testThreeAdd() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", ScalarType.INT, true);

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(column1, column2));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, ConstantOperator.createInt(3)));

        CallOperator add3 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add2, ConstantOperator.createInt(1)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(add1, add2, add3);

        List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);

        add3.setChild(0, columnRefFactory.getColumnRef(4));
        List<ScalarOperator> exceptResult = Lists.newArrayList(
                columnRefFactory.getColumnRef(3),
                columnRefFactory.getColumnRef(4),
                add3);

        assertEquals(exceptResult, newOperators);
    }

    @Test
    public void testNoRedundantCommonScalarOperators() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", ScalarType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("t3", ScalarType.INT, true);

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(1)));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, column2));

        CallOperator add3 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add2, column3));

        CallOperator multi = new CallOperator("multi", Type.INT,
                Lists.newArrayList(add3, ConstantOperator.createInt(2)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(add1, add3, multi);

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory, false);

        // FixMe(kks): This case could improve
        assertEquals(commonSubScalarOperators.size(), 3);
    }

    @Test
    public void testCollectCommonScalarOperators() {
        ColumnRefOperator column1 = columnRefFactory.create("a", ScalarType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("b", ScalarType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("c", ScalarType.INT, true);
        ColumnRefOperator column4 = columnRefFactory.create("d", ScalarType.INT, true);

        CallOperator addAB = new CallOperator("add", Type.INT,
                Lists.newArrayList(column1, column2));

        CallOperator addABC = new CallOperator("add", Type.INT,
                Lists.newArrayList(addAB, column3));

        CallOperator addBC = new CallOperator("add", Type.INT,
                Lists.newArrayList(column2, column3));

        CallOperator addBCD = new CallOperator("add", Type.INT,
                Lists.newArrayList(addBC, column4));

        List<ScalarOperator> oldOperators = Lists.newArrayList(addABC, addBCD);

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory, false);

        // FixMe(kks): could we improve this case?
        assertTrue(commonSubScalarOperators.isEmpty());
    }

    @Test
    public void testNonDeterministicFuncCommonUsed() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", ScalarType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("t3", ScalarType.INT, true);

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(column1, new CallOperator(FunctionSet.RANDOM, Type.DOUBLE, Lists.newArrayList())));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, column2));

        CallOperator add3 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add2, column3));

        CallOperator add4 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add3, ConstantOperator.createInt(1)));

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, ImmutableList.of(add1, add2, add3, add4),
                        columnRefFactory, false);
        assertTrue(commonSubScalarOperators.isEmpty());
    }

    @Test
    public void testNonDeterministicFuncNotCommonUsed() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", ScalarType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("t3", ScalarType.INT, true);

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(1)));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, column2));

        CallOperator add3 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add2, new CallOperator(FunctionSet.RANDOM, Type.DOUBLE, Lists.newArrayList())));

        CallOperator add4 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add3, column3));

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, ImmutableList.of(add1, add2, add3, add4),
                        columnRefFactory, false);
        assertEquals(2, commonSubScalarOperators.size());
    }

    @Test
    public void testLambdaFunctionWithoutLambdaArguments() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);
        ColumnRefOperator arg = columnRefFactory.create("x", ScalarType.INT, true, true);


        CallOperator multi = new CallOperator("multi", Type.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(2)));

        CallOperator multi1 = new CallOperator("multi", Type.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(2)));

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(multi, multi1));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, arg));
        // x-> t1 * 2 + t1 *2 + x
        List<ScalarOperator> oldOperators = Lists.newArrayList(add2);

        // reuse lambda argument non-related sub expressions : t1*2
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory, false);
        assertEquals(commonSubScalarOperators.size(), 1);
    }

    @Test
    public void testLambdaFunctionScalarOperatorsWithLambdaArguments() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", ScalarType.INT, true);
        ColumnRefOperator arg = columnRefFactory.create("x", ScalarType.INT, true, true);


        CallOperator multi = new CallOperator("multi", Type.INT,
                Lists.newArrayList(arg, ConstantOperator.createInt(2)));

        CallOperator multi1 = new CallOperator("multi", Type.INT,
                Lists.newArrayList(arg, ConstantOperator.createInt(2)));

        CallOperator add1 = new CallOperator("add", Type.INT,
                Lists.newArrayList(multi, multi1));

        CallOperator add2 = new CallOperator("add", Type.INT,
                Lists.newArrayList(add1, column1));
        // x-> x * 2 + x *2 + t1
        List<ScalarOperator> oldOperators = Lists.newArrayList(add2);

        // reuse lambda argument related sub expressions : x*2
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory, true);
        assertEquals(commonSubScalarOperators.size(), 1);

    }
}
