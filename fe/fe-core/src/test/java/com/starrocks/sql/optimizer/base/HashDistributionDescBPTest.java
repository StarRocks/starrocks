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
        Assertions.assertFalse(desc.isNative());
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
        Assertions.assertFalse(relaxDesc.isNative());
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
        Assertions.assertFalse(strictDesc.isNative());
        // The strict description should have the same source type
        Assertions.assertEquals(desc.getSourceType(), strictDesc.getSourceType());
    }

    @Test
    void testCanColocateWithSameProperties() {
        List<Integer> columns = Lists.newArrayList(1, 2);
        List<BucketProperty> bucketProperties = createBucketProperties(2);
        
        HashDistributionDescBP desc1 = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        HashDistributionDescBP desc2 = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        
        Assertions.assertTrue(desc1.canColocate(desc2));
    }

    @Test
    void testCanColocateWithDifferentSizes() {
        List<Integer> columns1 = Lists.newArrayList(1, 2);
        List<Integer> columns2 = Lists.newArrayList(1, 2, 3);
        
        List<BucketProperty> bucketProperties1 = createBucketProperties(2);
        List<BucketProperty> bucketProperties2 = createBucketProperties(3);
        
        HashDistributionDescBP desc1 = new HashDistributionDescBP(
                columns1, HashDistributionDesc.SourceType.LOCAL, bucketProperties1);
        HashDistributionDescBP desc2 = new HashDistributionDescBP(
                columns2, HashDistributionDesc.SourceType.LOCAL, bucketProperties2);
        
        Assertions.assertFalse(desc1.canColocate(desc2));
    }

    @Test
    void testCanColocateWithNonBucket() {
        List<Integer> columns = Lists.newArrayList(1, 2);
        List<BucketProperty> bucketProperties = createBucketProperties(2);
        
        HashDistributionDescBP desc1 = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        
        // Create a non-bucket local HashDistributionDesc
        HashDistributionDesc nonBucketLocalDesc = new HashDistributionDesc(
                columns, HashDistributionDesc.SourceType.LOCAL);
        
        Assertions.assertFalse(desc1.canColocate(nonBucketLocalDesc));
        Assertions.assertFalse(nonBucketLocalDesc.canColocate(desc1));
    }

    @Test
    void testCanColocateWithDifferentBucketProperties() {
        List<Integer> columns = Lists.newArrayList(1, 2);
        
        List<BucketProperty> bucketProperties1 = createBucketProperties(2);
        
        // Create different bucket properties that won't satisfy
        List<BucketProperty> bucketProperties2 = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            Column column = new Column("col" + i, Type.INT);
            // Different bucket count
            bucketProperties2.add(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 20, column));
        }
        
        HashDistributionDescBP desc1 = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties1);
        HashDistributionDescBP desc2 = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties2);
        
        Assertions.assertFalse(desc1.canColocate(desc2));
    }

    @Test
    void testCanColocateWithSelf() {
        List<Integer> columns = Lists.newArrayList(1, 2);
        List<BucketProperty> bucketProperties = createBucketProperties(2);
        
        HashDistributionDescBP desc = new HashDistributionDescBP(
                columns, HashDistributionDesc.SourceType.LOCAL, bucketProperties);
        
        Assertions.assertTrue(desc.canColocate(desc));
    }

}