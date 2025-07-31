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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.connector.BucketProperty;
import com.starrocks.thrift.TBucketFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class HashDistributionDescBPTest {

    private List<BucketProperty> createBucketProperties(int count) {
        List<BucketProperty> bucketProperties = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            Column column = new Column("col" + i, Type.INT);
            bucketProperties.add(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, column));
        }
        return bucketProperties;
    }

    @Test
    void testConstructorWithValidArguments() {
        List<Integer> columns = Lists.newArrayList(1, 2, 3);
        List<BucketProperty> bucketProperties = createBucketProperties(3);
        
        HashDistributionDescBP desc = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        
        Assertions.assertNotNull(desc);
        Assertions.assertEquals(bucketProperties, desc.getBucketProperties());
        Assertions.assertEquals(columns.size(), desc.getBucketProperties().size());
        Assertions.assertTrue(desc.isBucketLocal());
    }

    @Test
    void testConstructorWithMismatchedSizes() {
        List<Integer> columns = Lists.newArrayList(1, 2, 3);
        List<BucketProperty> bucketProperties = createBucketProperties(2); // Different size

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new HashDistributionDescBP(columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        });
    }

    @Test
    void testGetNullRelaxDesc() {
        List<Integer> columns = Lists.newArrayList(1, 2);
        List<BucketProperty> bucketProperties = createBucketProperties(2);
        
        HashDistributionDescBP desc = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        
        HashDistributionDescBP relaxDesc = desc.getNullRelaxDesc();

        Assertions.assertNotNull(relaxDesc);
        Assertions.assertEquals(bucketProperties, relaxDesc.getBucketProperties());
        Assertions.assertTrue(relaxDesc.isBucketLocal());
        // The relaxed description should have the same source type
        Assertions.assertEquals(desc.getSourceType(), relaxDesc.getSourceType());
    }

    @Test
    void testGetNullStrictDesc() {
        List<Integer> columns = Lists.newArrayList(1, 2);
        List<BucketProperty> bucketProperties = createBucketProperties(2);
        
        HashDistributionDescBP desc = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        
        HashDistributionDescBP strictDesc = desc.getNullStrictDesc();

        Assertions.assertNotNull(strictDesc);
        Assertions.assertEquals(bucketProperties, strictDesc.getBucketProperties());
        Assertions.assertTrue(strictDesc.isBucketLocal());
        // The strict description should have the same source type
        Assertions.assertEquals(desc.getSourceType(), strictDesc.getSourceType());
    }

}