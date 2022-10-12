// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
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
                ScalarOperatorsReuse.collectCommonSubScalarOperators(oldOperators, columnRefFactory);

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
                ScalarOperatorsReuse.collectCommonSubScalarOperators(oldOperators, columnRefFactory);

        // FixMe(kks): could we improve this case?
        assertTrue(commonSubScalarOperators.isEmpty());
    }
}
