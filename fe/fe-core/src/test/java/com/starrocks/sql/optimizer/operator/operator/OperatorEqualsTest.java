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

package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
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
                10, new BinaryPredicateOperator(BinaryType.EQ,
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
