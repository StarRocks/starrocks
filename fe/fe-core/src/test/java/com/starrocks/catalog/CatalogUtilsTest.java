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

import com.starrocks.common.FeConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.when;

public class CatalogUtilsTest {

    @Mock
    private OlapTable olapTable;

    @Mock
    private PartitionInfo partitionInfo;

    @Mock
    private Partition partition;

    @Mock
    private PhysicalPartition physicalPartition;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCalAvgBucketNumOfRecentPartitions_FewerPartitionsThanRecent() {
        when(olapTable.getPartitions()).thenReturn(new ArrayList<>());
        when(olapTable.getPartitionInfo()).thenReturn(partitionInfo);
        when(partitionInfo.isPartitioned()).thenReturn(false);

        int bucketNum = CatalogUtils.calAvgBucketNumOfRecentPartitions(olapTable, 5, true);

        assertEquals(FeConstants.DEFAULT_UNPARTITIONED_TABLE_BUCKET_NUM, bucketNum);
    }

    @Test
    public void testCalAvgBucketNumOfRecentPartitions_CalculateByDataSize() {
        List<Partition> partitions = new ArrayList<>();
        partitions.add(partition);
        when(olapTable.getPartitions()).thenReturn(partitions);
        when(olapTable.getRecentPartitions(anyInt())).thenReturn(partitions);
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(physicalPartition.getVisibleVersion()).thenReturn(2L);
        when(partition.getDataSize()).thenReturn(2L * FeConstants.AUTO_DISTRIBUTION_UNIT);

        int bucketNum = CatalogUtils.calAvgBucketNumOfRecentPartitions(olapTable, 1, true);

        assertEquals(2, bucketNum); // 2 tablets based on 2GB size
    }

    @Test
    public void testDivisibleByTwo() {
        Assertions.assertEquals(1, CatalogUtils.divisibleBucketNum(1));
        Assertions.assertEquals(2, CatalogUtils.divisibleBucketNum(2));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(3));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(4));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(5));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(6));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(7));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(8));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(9));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(10));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(11));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(12));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(13));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(14));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(15));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(16));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(17));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(18));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(19));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(20));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(21));
    }
}
