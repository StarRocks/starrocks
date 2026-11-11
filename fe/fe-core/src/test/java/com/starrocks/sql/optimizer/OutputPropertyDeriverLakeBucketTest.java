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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.Column;
import com.starrocks.connector.BucketProperty;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionDescBP;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TBucketFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.type.IntegerType.INT;

public class OutputPropertyDeriverLakeBucketTest {

    @SuppressWarnings("unchecked")
    private static Optional<HashDistributionDesc> compute(HashDistributionDesc require, List<BucketProperty> bps,
                                                          Map<ColumnRefOperator, Column> map) throws Exception {
        Method method = OutputPropertyDeriver.class.getDeclaredMethod("computeLakeHashDistributionDesc",
                HashDistributionDesc.class, List.class, Map.class);
        method.setAccessible(true);
        return (Optional<HashDistributionDesc>) method.invoke(new OutputPropertyDeriver(null, null, List.of()),
                require, bps, map);
    }

    @Test
    public void testDistributionColumnsFollowRequiredOrder() throws Exception {
        // spec [bucket(c1, 4), bucket(c2, 8)]; the join requires c2 before c1, which is not the column-id order.
        Column c1 = new Column("c1", INT);
        Column c2 = new Column("c2", INT);
        List<BucketProperty> bps = List.of(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 4, c1),
                new BucketProperty(TBucketFunction.MURMUR3_X86_32, 8, c2));
        Map<ColumnRefOperator, Column> map = new HashMap<>();
        map.put(new ColumnRefOperator(3, INT, "c1", true), c1);
        map.put(new ColumnRefOperator(5, INT, "c2", true), c2);

        HashDistributionDesc require = new HashDistributionDesc(List.of(5, 3), HashDistributionDesc.SourceType.SHUFFLE_JOIN);
        HashDistributionDescBP desc = (HashDistributionDescBP) compute(require, bps, map).orElseThrow();

        // The i-th distribution column is hashed with the i-th bucket property: both in the required order.
        Assertions.assertEquals(List.of(5, 3), desc.getDistributionCols().stream().map(DistributionCol::getColId).toList());
        Assertions.assertEquals(List.of(8, 4), desc.getBucketProperties().stream().map(BucketProperty::getBucketNum).toList());
    }

    @Test
    public void testNonBucketColumnsAreDropped() throws Exception {
        Column c1 = new Column("c1", INT);
        List<BucketProperty> bps = List.of(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 4, c1));
        Map<ColumnRefOperator, Column> map = new HashMap<>();
        map.put(new ColumnRefOperator(3, INT, "c1", true), c1);
        map.put(new ColumnRefOperator(5, INT, "c9", true), new Column("c9", INT));

        HashDistributionDesc require = new HashDistributionDesc(List.of(5, 3), HashDistributionDesc.SourceType.SHUFFLE_JOIN);
        HashDistributionDescBP desc = (HashDistributionDescBP) compute(require, bps, map).orElseThrow();
        Assertions.assertEquals(List.of(3), desc.getDistributionCols().stream().map(DistributionCol::getColId).toList());

        HashDistributionDesc onlyOther = new HashDistributionDesc(List.of(5), HashDistributionDesc.SourceType.SHUFFLE_JOIN);
        Assertions.assertTrue(compute(onlyOther, bps, map).isEmpty());
    }
}
