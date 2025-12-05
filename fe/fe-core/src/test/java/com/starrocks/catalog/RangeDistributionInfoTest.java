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

package com.starrocks.catalog;

import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.RangeDistributionDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangeDistributionInfoTest {

    @Test
    public void testConstructor() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        Assertions.assertNotNull(rangeDistributionInfo);
        Assertions.assertEquals(DistributionInfo.DistributionInfoType.RANGE, rangeDistributionInfo.getType());
    }

    @Test
    public void testSupportColocate() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        Assertions.assertFalse(rangeDistributionInfo.supportColocate());
    }

    @Test
    public void testGetBucketNum() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        Assertions.assertEquals(1, rangeDistributionInfo.getBucketNum());
    }

    @Test
    public void testSetBucketNum() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        // setBucketNum should do nothing without throwing exception
        rangeDistributionInfo.setBucketNum(10);
        // bucket num should still be 1
        Assertions.assertEquals(1, rangeDistributionInfo.getBucketNum());
    }

    @Test
    public void testGetDistributionColumns() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        List<ColumnId> columns = rangeDistributionInfo.getDistributionColumns();
        Assertions.assertNotNull(columns);
        Assertions.assertTrue(columns.isEmpty());
    }

    @Test
    public void testToDistributionDesc() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        Map<ColumnId, Column> schema = new HashMap<>();
        DistributionDesc desc = rangeDistributionInfo.toDistributionDesc(schema);
        Assertions.assertNotNull(desc);
        Assertions.assertTrue(desc instanceof RangeDistributionDesc);
    }

    @Test
    public void testToSql() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        Map<ColumnId, Column> schema = new HashMap<>();
        String sql = rangeDistributionInfo.toSql(schema);
        Assertions.assertNotNull(sql);
        Assertions.assertEquals("", sql);
    }

    @Test
    public void testCopy() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        RangeDistributionInfo copy = rangeDistributionInfo.copy();
        Assertions.assertNotNull(copy);
        Assertions.assertNotSame(rangeDistributionInfo, copy);
        Assertions.assertEquals(rangeDistributionInfo, copy);
        Assertions.assertEquals(rangeDistributionInfo.getType(), copy.getType());
    }

    @Test
    public void testHashCode() {
        RangeDistributionInfo rangeDistributionInfo1 = new RangeDistributionInfo();
        RangeDistributionInfo rangeDistributionInfo2 = new RangeDistributionInfo();
        Assertions.assertEquals(rangeDistributionInfo1.hashCode(), rangeDistributionInfo2.hashCode());
    }

    @Test
    public void testEquals() {
        RangeDistributionInfo rangeDistributionInfo1 = new RangeDistributionInfo();
        RangeDistributionInfo rangeDistributionInfo2 = new RangeDistributionInfo();

        // Test same object
        Assertions.assertTrue(rangeDistributionInfo1.equals(rangeDistributionInfo1));

        // Test equal objects
        Assertions.assertTrue(rangeDistributionInfo1.equals(rangeDistributionInfo2));
        Assertions.assertTrue(rangeDistributionInfo2.equals(rangeDistributionInfo1));

        // Test null
        Assertions.assertFalse(rangeDistributionInfo1.equals(null));

        // Test different type
        Assertions.assertFalse(rangeDistributionInfo1.equals(new Object()));
    }

    @Test
    public void testGetDistributionKey() {
        RangeDistributionInfo rangeDistributionInfo = new RangeDistributionInfo();
        Map<ColumnId, Column> schema = new HashMap<>();
        String key = rangeDistributionInfo.getDistributionKey(schema);
        Assertions.assertNotNull(key);
        Assertions.assertEquals("", key);
    }
}
