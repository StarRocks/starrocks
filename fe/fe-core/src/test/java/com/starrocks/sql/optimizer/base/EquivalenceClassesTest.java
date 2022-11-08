// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class EquivalenceClassesTest {
    @Test
    public void test() {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        EquivalenceClasses ec = new EquivalenceClasses();
        ColumnRefOperator columnRef1 = columnRefFactory.create("column1", Type.INT, true);
        ColumnRefOperator columnRef2 = columnRefFactory.create("column2", Type.INT, true);
        ColumnRefOperator columnRef3 = columnRefFactory.create("column3", Type.INT, true);
        ColumnRefOperator columnRef4 = columnRefFactory.create("column4", Type.INT, true);
        ec.addEquivalence(columnRef1, columnRef2);
        ec.addEquivalence(columnRef1, columnRef3);
        ec.addEquivalence(columnRef4, columnRef3);

        Set<ColumnRefOperator> columnSet = ec.getEquivalenceClass(columnRef1);
        Assert.assertEquals(4, columnSet.size());
        List<Set<ColumnRefOperator>> columnSetList = ec.getEquivalenceClasses();
        Assert.assertEquals(4, columnSetList.size());
    }
}
