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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PartitionUtilsTest {

    @Test
    public void testConvertDateLiteralToDouble() throws Exception {
        Object result = PartitionUtils.convertDateLiteralToNumber(
                new DateLiteral("2015-03-01", ScalarType.DATE));
        assertEquals(1031777L, result);

        result = PartitionUtils.convertDateLiteralToNumber(
                new DateLiteral("2015-03-01 12:00:00", ScalarType.DATETIME));
        assertEquals(20150301120000L, result);
    }

    @Test
    public void testClearTabletsFromInvertedIndex() throws Exception {
        List<Partition> partitions = Lists.newArrayList();
        MaterializedIndex materializedIndex = new MaterializedIndex();
        HashDistributionInfo distributionInfo =
                new HashDistributionInfo(1, Lists.newArrayList(new Column("id", Type.BIGINT)));

        Partition p1 = new Partition(10001L, "p1", materializedIndex, distributionInfo);
        partitions.add(p1);
        PartitionUtils.clearTabletsFromInvertedIndex(partitions);
    }
}