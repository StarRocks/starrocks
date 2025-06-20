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
import com.starrocks.common.util.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PRangeCellTest {

    private PartitionKey createPartitionKey(String key) throws AnalysisException {
        return PartitionKey.ofDateTime(DateUtils.parseStrictDateTime(key));
    }

    private PRangeCell buildPartitionRange(String start, String end) throws AnalysisException {
        return new PRangeCell(Range.closedOpen(createPartitionKey(start), createPartitionKey(end)));
    }

    @Test
    public void testComparison() throws AnalysisException {
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        final PartitionKey key3 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 12, 0, 0));
        final PartitionKey key4 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 13, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PRangeCell cell1 = new PRangeCell(r1);
        Assert.assertEquals(0, cell1.compareTo(cell1));

        Range<PartitionKey> r2 = Range.closed(key2, key3);
        PRangeCell cell2 = new PRangeCell(r2);
        Assert.assertEquals(-1, cell1.compareTo(cell2));
        Assert.assertEquals(1, cell2.compareTo(cell1));

        Range<PartitionKey> r3 = Range.closed(key3, key4);
        PRangeCell cell3 = new PRangeCell(r3);
        Assert.assertEquals(-1, cell2.compareTo(cell3));
        Assert.assertEquals(1, cell3.compareTo(cell2));
        Assert.assertEquals(-1, cell1.compareTo(cell3));
        Assert.assertEquals(1, cell3.compareTo(cell1));

        Range<PartitionKey> r4 = Range.closed(key1, key4);
        PRangeCell cell4 = new PRangeCell(r4);
        PRangeCell cell5 = new PRangeCell(r3);
        PRangeCell cell6 = new PRangeCell(r1);
        Assert.assertEquals(-1, cell4.compareTo(cell5));
        Assert.assertEquals(1, cell4.compareTo(cell6));
        Assert.assertEquals(1, cell5.compareTo(cell6));
    }

    @Test
    public void testBinarySearch() throws AnalysisException {
        List<PRangeCell> srcs = Arrays.asList(
                buildPartitionRange("00000101", "20230801"),
                buildPartitionRange("20230801", "20230802"),
                buildPartitionRange("20230802", "20230803"),
                buildPartitionRange("20230803", "20230804"),
                buildPartitionRange("20230804", "20230805"),
                buildPartitionRange("20230805", "20991231"));

        Collections.sort(srcs, PRangeCell::compareTo);

        Assert.assertEquals(1, Collections.binarySearch(srcs, buildPartitionRange("20230801", "20230802")));
    }
}
