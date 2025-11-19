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

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

public class PCellWithNameTest {
    @Test
    public void testComparison() throws AnalysisException {
        final String partitionName = "p1";
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        final PartitionKey key3 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 12, 0, 0));
        final PartitionKey key4 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 13, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PCellWithName cell1 = new PCellWithName(partitionName, new PRangeCell(r1));
        Assertions.assertEquals(0, cell1.compareTo(cell1));

        Range<PartitionKey> r2 = Range.closed(key2, key3);
        PCellWithName cell2 = new PCellWithName(partitionName, new PRangeCell(r2));
        Assertions.assertEquals(-1, cell1.compareTo(cell2));
        Assertions.assertEquals(1, cell2.compareTo(cell1));

        Range<PartitionKey> r3 = Range.closed(key3, key4);
        PCellWithName cell3 = new PCellWithName(partitionName, new PRangeCell(r3));
        Assertions.assertEquals(-1, cell2.compareTo(cell3));
        Assertions.assertEquals(1, cell3.compareTo(cell2));
        Assertions.assertEquals(-1, cell1.compareTo(cell3));
        Assertions.assertEquals(1, cell3.compareTo(cell1));

        Range<PartitionKey> r4 = Range.closed(key1, key4);
        PCellWithName cell4 = new PCellWithName(partitionName, new PRangeCell(r4));
        PCellWithName cell5 = new PCellWithName(partitionName, new PRangeCell(r3));
        PCellWithName cell6 = new PCellWithName(partitionName, new PRangeCell(r1));
        Assertions.assertEquals(-1, cell4.compareTo(cell5));
        Assertions.assertEquals(1, cell4.compareTo(cell6));
        Assertions.assertEquals(1, cell5.compareTo(cell6));
    }
}
