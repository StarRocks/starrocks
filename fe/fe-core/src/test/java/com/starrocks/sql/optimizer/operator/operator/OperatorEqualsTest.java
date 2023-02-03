// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

public class OperatorEqualsTest {

    @Test
    public void testTopNOperator() {
        LogicalTopNOperator logicalA = LogicalTopNOperator.builder()
                .setProjection(new Projection(Maps.newHashMap())).build();
        LogicalTopNOperator logicalB = LogicalTopNOperator.builder().build();
        Assert.assertNotEquals(logicalA.hashCode(), logicalB.hashCode());
        Assert.assertNotEquals(logicalA, logicalB);

        PhysicalTopNOperator physicalA = new PhysicalTopNOperator(null, 10, 10, null,
                0, null, null, true, true, null, null);
        PhysicalTopNOperator physicalB = new PhysicalTopNOperator(null, 10, 10, null,
                0, null, null, true, true,
                null, new Projection(Maps.newHashMap()));
        Assert.assertNotEquals(physicalA.hashCode(), physicalB.hashCode());
        Assert.assertNotEquals(physicalA, physicalB);
    }

    @Test
    public void testUnionOperator() {
        LogicalUnionOperator logicalA = LogicalUnionOperator.builder()
                .setProjection(new Projection(Maps.newHashMap())).build();
        LogicalUnionOperator logicalB = LogicalUnionOperator.builder().build();
        Assert.assertNotEquals(logicalA.hashCode(), logicalB.hashCode());
        Assert.assertNotEquals(logicalA, logicalB);

        PhysicalUnionOperator physicalA = new PhysicalUnionOperator(null, null, true,
                10, new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                        Lists.newArrayList(
                                ConstantOperator.createBoolean(true),
                                ConstantOperator.createBoolean(false)
                        )), null);

        PhysicalUnionOperator physicalB = new PhysicalUnionOperator(null, null, true,
                10, null, null);

        Assert.assertNotEquals(physicalA.hashCode(), physicalB.hashCode());
        Assert.assertNotEquals(physicalA, physicalB);
    }
}
