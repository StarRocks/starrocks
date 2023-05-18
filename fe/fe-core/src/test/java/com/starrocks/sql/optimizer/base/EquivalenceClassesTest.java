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


package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Sets;
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
        Assert.assertEquals(1, columnSetList.size());
        Set<ColumnRefOperator> columnSet2 = Sets.newHashSet(columnSetList.get(0));
        Assert.assertEquals(columnSet, columnSet2);

        ColumnRefOperator columnRef5 = columnRefFactory.create("column5", Type.INT, true);
        ColumnRefOperator columnRef6 = columnRefFactory.create("column6", Type.INT, true);
        ColumnRefOperator columnRef7 = columnRefFactory.create("column7", Type.INT, true);
        ec.addEquivalence(columnRef5, columnRef6);
        ec.addEquivalence(columnRef5, columnRef7);
        Assert.assertEquals(ec.getEquivalenceClass(columnRef6), ec.getEquivalenceClass(columnRef7));

        EquivalenceClasses ec2 = new EquivalenceClasses();
        ec2.addEquivalence(columnRef1, columnRef2);
        ec2.addEquivalence(columnRef1, columnRef3);

        ec2.addEquivalence(columnRef5, columnRef6);
        ec2.addEquivalence(columnRef5, columnRef7);
        ec2.addEquivalence(columnRef1, columnRef6);
        Assert.assertEquals(ec2.getEquivalenceClass(columnRef1), ec2.getEquivalenceClass(columnRef5));
        Assert.assertEquals(6, ec2.getEquivalenceClass(columnRef1).size());
    }
}
