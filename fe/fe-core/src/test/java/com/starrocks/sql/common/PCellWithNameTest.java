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
import com.starrocks.planner.SlotId;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

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

    @Test
    public void testPCellCacheKeyEqualsAndHashCode() throws Exception {
        final String partitionName = "p1";
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PCellWithName pcell = new PCellWithName(partitionName, new PRangeCell(r1));
        PCellWithName samePCell = new PCellWithName(partitionName, new PRangeCell(r1));
        PCellWithName differentPCell = new PCellWithName("p2", new PRangeCell(r1));

        PCellWithName.PCellCacheKey keyA = new PCellWithName.PCellCacheKey(null, pcell);
        PCellWithName.PCellCacheKey keyB = new PCellWithName.PCellCacheKey(null, samePCell);
        PCellWithName.PCellCacheKey keyC = new PCellWithName.PCellCacheKey(null, differentPCell);

        Assertions.assertEquals(keyA, keyA);
        Assertions.assertEquals(keyA, keyB);
        Assertions.assertEquals(keyA.hashCode(), keyB.hashCode());
        Assertions.assertNotEquals(keyA, keyC);
    }

    @Test
    public void testPCellCacheKeyNotEqual() throws Exception {
        final String partitionName = "p1";
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PCellWithName pcell1 = new PCellWithName(partitionName, new PRangeCell(r1));
        PCellWithName pcell2 = new PCellWithName("p2", new PRangeCell(r1));

        PCellWithName.PCellCacheKey keyNull = new PCellWithName.PCellCacheKey(null, pcell1);
        PCellWithName.PCellCacheKey keyOtherPCell = new PCellWithName.PCellCacheKey(null, pcell2);

        Assertions.assertNotEquals(keyNull, keyOtherPCell);
    }

    @Test
    public void testPCellWithNormOf() throws Exception {
        final String partitionName = "p1";
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PCellWithName base = new PCellWithName(partitionName, new PRangeCell(r1));
        PCellWithName normalized = new PCellWithName("p_norm", new PRangeCell(r1));

        PCellWithName.PCellWithNorm norm = PCellWithName.PCellWithNorm.of(base, normalized);
        Assertions.assertEquals(base, norm.basePCell());
        Assertions.assertEquals(normalized, norm.normalized());
    }

    @Test
    public void testNormalizePCellWithNames1() throws Exception {
        final String partitionName1 = "p1";
        final String partitionName2 = "p2";
        final PartitionKey k1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey k2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        final PartitionKey k3 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 12, 0, 0));
        final PartitionKey k4 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 13, 0, 0));

        Range<PartitionKey> r1 = Range.closed(k1, k2);
        Range<PartitionKey> r2 = Range.closed(k3, k4);
        PCellWithName p1 = new PCellWithName(partitionName1, new PRangeCell(r1));
        PCellWithName p2 = new PCellWithName(partitionName2, new PRangeCell(r2));

        // Provide a minimal PCellSortedSet by overriding getPartitions()
        PCellSortedSet rangeMap = new PCellSortedSet(new TreeSet<>()) {
            @Override
            public NavigableSet<PCellWithName> getPartitions() {
                return new TreeSet<>(List.of(p1, p2));
            }
        };

        List<PCellWithName.PCellWithNorm> result = PCellWithName.normalizePCellWithNames(null, null, rangeMap, null);
        Assertions.assertEquals(2, result.size());
        Assertions.assertSame(result.get(0).basePCell(), result.get(0).normalized());
        Assertions.assertSame(result.get(1).basePCell(), result.get(1).normalized());
    }

    @Test
    public void testNormalizePCellWithNames2() throws Exception {
        final String partitionName1 = "p1";
        final PartitionKey k1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey k2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        Range<PartitionKey> r1 = Range.closed(k1, k2);
        PCellWithName p1 = new PCellWithName(partitionName1, new PRangeCell(r1));

        PCellSortedSet rangeMap = new PCellSortedSet(new TreeSet<>()) {
            @Override
            public NavigableSet<PCellWithName> getPartitions() {
                return new TreeSet(List.of(p1));
            }
        };

        Expr slotRef = new SlotRef(new SlotId(9));
        List<PCellWithName.PCellWithNorm> result = PCellWithName.normalizePCellWithNames(null, null, rangeMap, slotRef);
        Assertions.assertEquals(1, result.size());
        Assertions.assertSame(result.get(0).basePCell(), result.get(0).normalized());
    }

    @Test
    public void testtoNormalizedCell() throws Exception {
        final String partitionName = "p1";
        final PartitionKey k1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey k2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        Range<PartitionKey> r1 = Range.closed(k1, k2);
        PCellWithName pcell = new PCellWithName(partitionName, new PRangeCell(r1));

        Assertions.assertSame(pcell, PCellWithName.toNormalizedCell(pcell, null));
        Assertions.assertSame(pcell, PCellWithName.toNormalizedCell(pcell, new SlotRef(new SlotId(1))));
    }
}
