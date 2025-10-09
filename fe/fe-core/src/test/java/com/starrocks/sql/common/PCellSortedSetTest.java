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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class PCellSortedSetTest extends MVTestBase  {
    private static final PartitionKey PARTITION_KEY11 = new PartitionKey(
            ImmutableList.of(new DateLiteral(1998, 1, 1)),
            ImmutableList.of(PrimitiveType.DATE));
    private static final PartitionKey PARTITION_KEY12 = new PartitionKey(
            ImmutableList.of(new DateLiteral(1999, 1, 1)),
            ImmutableList.of(PrimitiveType.DATE));
    private static final Range<PartitionKey> RANGE1 = Range.closed(PARTITION_KEY11, PARTITION_KEY12);

    private static final PartitionKey PARTITION_KEY21 = new PartitionKey(
            ImmutableList.of(new DateLiteral(1999, 1, 1)),
            ImmutableList.of(PrimitiveType.DATE));
    private static final PartitionKey PARTITION_KEY22 = new PartitionKey(
            ImmutableList.of(new DateLiteral(2000, 1, 1)),
            ImmutableList.of(PrimitiveType.DATE));
    private static final Range<PartitionKey> RANGE2 = Range.closed(PARTITION_KEY21, PARTITION_KEY22);

    @Test
    public void testAddAndContainsWithPListCell() {
        PCellSortedSet set = PCellSortedSet.of();
        PListCell cell1 = new PListCell("1");
        PListCell cell2 = new PListCell("2");
        PCellWithName p1 = PCellWithName.of("p1", cell1);
        PCellWithName p2 = PCellWithName.of("p2", cell2);

        set.add(p1);
        set.add(p2);

        Assertions.assertTrue(set.contains(p1));
        Assertions.assertTrue(set.contains(p2));
        Assertions.assertEquals(2, set.size());
    }

    @Test
    public void testAddAndRemoveWithPRangeCell() {
        PCellSortedSet set = PCellSortedSet.of();
        PRangeCell cell1 = new PRangeCell(RANGE1);
        PRangeCell cell2 = new PRangeCell(RANGE2);
        PCellWithName p1 = PCellWithName.of("range1", cell1);
        PCellWithName p2 = PCellWithName.of("range2", cell2);

        set.add(p1);
        set.add(p2);

        Assertions.assertTrue(set.contains(p1));
        Assertions.assertTrue(set.contains(p2));
        Assertions.assertEquals(2, set.size());

        set.remove(p1);
        Assertions.assertFalse(set.contains(p1));
        Assertions.assertTrue(set.contains(p2));
        Assertions.assertEquals(1, set.size());
    }

    @Test
    public void testLimitAndSkip() {
        PCellSortedSet set = PCellSortedSet.of();
        for (int i = 0; i < 5; i++) {
            set.add(PCellWithName.of("p" + i, new PListCell(String.valueOf(i))));
        }
        PCellSortedSet limited = set.limit(2);
        Assertions.assertEquals(2, limited.size());
        Assertions.assertTrue(limited.getPartitionNames().contains("p3"));
        Assertions.assertTrue(limited.getPartitionNames().contains("p4"));

        PCellSortedSet skipped = set.skip(3);
        Assertions.assertEquals(2, skipped.size());
        Assertions.assertTrue(skipped.getPartitionNames().contains("p3"));
        Assertions.assertTrue(skipped.getPartitionNames().contains("p4"));
    }

    private Range<PartitionKey> createRange(int start, int end) {
        PartitionKey startKey = new PartitionKey(
                Collections.singletonList(new IntLiteral(start)),
                Collections.singletonList(PrimitiveType.INT));
        PartitionKey endKey = new PartitionKey(
                Collections.singletonList(new IntLiteral(end)),
                Collections.singletonList(PrimitiveType.INT));
        return Range.closed(startKey, endKey);
    }

    @Test
    public void testToStringCompaction() {
        PCellSortedSet set = PCellSortedSet.of();
        for (int i = 1; i <= 5; i++) {
            set.add(PCellWithName.of("p" + i, new PRangeCell(createRange(i, i + 10))));
        }
        String str = set.toString();
        // Should compact to something like "p1~p5" if compaction logic is correct
        Assertions.assertTrue(str.contains("p1") && str.contains("p5"));
    }

    @Test
    public void testToStringWithOnlyPListCell() {
        PCellSortedSet set = PCellSortedSet.of();
        set.add(PCellWithName.of("l1", new PListCell("a")));
        set.add(PCellWithName.of("l2", new PListCell("b")));
        set.add(PCellWithName.of("l3", new PListCell("c")));

        String str = set.toString();

        System.out.println(str);
        // Should list all names, no compaction for non-numeric suffixes
        Assertions.assertTrue(str.contains("l1"));
        Assertions.assertTrue(str.contains("l2"));
        Assertions.assertTrue(str.contains("l3"));
    }

    @Test
    public void testToStringEmptySet() {
        PCellSortedSet set = PCellSortedSet.of();
        String str = set.toString();
        Assertions.assertEquals("", str);
    }

    @Test
    public void testToStringWithMaxLenLimit() {
        // Set up a set with more elements than max_mv_task_run_meta_message_values_length
        int maxLen = 4; // Simulate a small config for test
        int originalMaxLen = com.starrocks.common.Config.max_mv_task_run_meta_message_values_length;
        com.starrocks.common.Config.max_mv_task_run_meta_message_values_length = maxLen;
        try {
            PCellSortedSet set = PCellSortedSet.of();
            for (int i = 1; i <= 10; i++) {
                set.add(PCellWithName.of("p" + i, new PRangeCell(createRange(i, i + 1))));
            }
            String str = set.toString();
            // Should contain prefix, ..., and suffix
            Assertions.assertTrue(str.contains("..."), "String should contain ellipsis for compaction");
            // Should contain first maxLen/2 and last maxLen/2 names
            int half = maxLen / 2;
            for (int i = 1; i <= half; i++) {
                Assertions.assertTrue(str.contains("p" + i), "Should contain prefix p" + i);
            }
            for (int i = 10 - half + 1; i <= 10; i++) {
                Assertions.assertTrue(str.contains("p" + i), "Should contain suffix p" + i);
            }
        } finally {
            com.starrocks.common.Config.max_mv_task_run_meta_message_values_length = originalMaxLen;
        }
    }

    @Test
    public void testToStringWithExactlyMaxLen() {
        int maxLen = 6;
        int originalMaxLen = com.starrocks.common.Config.max_mv_task_run_meta_message_values_length;
        com.starrocks.common.Config.max_mv_task_run_meta_message_values_length = maxLen;
        try {
            PCellSortedSet set = PCellSortedSet.of();
            for (int i = 1; i <= maxLen; i++) {
                set.add(PCellWithName.of("p" + i, new PRangeCell(createRange(i, i + 1))));
            }
            String str = set.toString();
            // Should not contain ellipsis, should list all names
            Assertions.assertFalse(str.contains("..."));
            for (int i = 1; i <= maxLen; i++) {
                Assertions.assertTrue(str.contains("p" + i));
            }
        } finally {
            com.starrocks.common.Config.max_mv_task_run_meta_message_values_length = originalMaxLen;
        }
    }

    @Test
    public void testToStringWithSingleElement() {
        PCellSortedSet set = PCellSortedSet.of();
        set.add(PCellWithName.of("single", new PRangeCell(createRange(1, 2))));
        String str = set.toString();
        Assertions.assertEquals("single", str);
    }

    @Test
    public void testToStringWithNonNumericNames() {
        PCellSortedSet set = PCellSortedSet.of();
        set.add(PCellWithName.of("alpha", new PListCell("a")));
        set.add(PCellWithName.of("beta", new PListCell("b")));
        set.add(PCellWithName.of("gamma", new PListCell("c")));
        String str = set.toString();
        // Should list all names, no compaction for non-numeric
        Assertions.assertTrue(str.contains("alpha"));
        Assertions.assertTrue(str.contains("beta"));
        Assertions.assertTrue(str.contains("gamma"));
        Assertions.assertFalse(str.contains("..."));
    }
}