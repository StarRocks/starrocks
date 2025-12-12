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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

public class PRangeCellPlusTest {
    @Test
    public void testComparison() throws AnalysisException {
        final String partitionName = "p1";
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        final PartitionKey key3 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 12, 0, 0));
        final PartitionKey key4 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 13, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PRangeCellPlus cell1 = new PRangeCellPlus(partitionName, r1);
        Assertions.assertEquals(0, cell1.compareTo(cell1));

        Range<PartitionKey> r2 = Range.closed(key2, key3);
        PRangeCellPlus cell2 = new PRangeCellPlus(partitionName, r2);
        Assertions.assertEquals(-1, cell1.compareTo(cell2));
        Assertions.assertEquals(1, cell2.compareTo(cell1));

        Range<PartitionKey> r3 = Range.closed(key3, key4);
        PRangeCellPlus cell3 = new PRangeCellPlus(partitionName, r3);
        Assertions.assertEquals(-1, cell2.compareTo(cell3));
        Assertions.assertEquals(1, cell3.compareTo(cell2));
        Assertions.assertEquals(-1, cell1.compareTo(cell3));
        Assertions.assertEquals(1, cell3.compareTo(cell1));

        Range<PartitionKey> r4 = Range.closed(key1, key4);
        PRangeCellPlus cell4 = new PRangeCellPlus(partitionName, r4);
        PRangeCellPlus cell5 = new PRangeCellPlus(partitionName, r3);
        PRangeCellPlus cell6 = new PRangeCellPlus(partitionName, r1);
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
        PRangeCellPlus pcell = new PRangeCellPlus(partitionName, new PRangeCell(r1));
        PRangeCellPlus samePCell = new PRangeCellPlus(partitionName, new PRangeCell(r1));
        PRangeCellPlus differentPCell = new PRangeCellPlus("p2", new PRangeCell(r1));

        PRangeCellPlus.PCellCacheKey keyA = new PRangeCellPlus.PCellCacheKey(null, pcell);
        PRangeCellPlus.PCellCacheKey keyB = new PRangeCellPlus.PCellCacheKey(null, samePCell);
        PRangeCellPlus.PCellCacheKey keyC = new PRangeCellPlus.PCellCacheKey(null, differentPCell);

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
        PRangeCellPlus pcell1 = new PRangeCellPlus(partitionName, new PRangeCell(r1));
        PRangeCellPlus pcell2 = new PRangeCellPlus("p2", new PRangeCell(r1));

        PRangeCellPlus.PCellCacheKey keyNull = new PRangeCellPlus.PCellCacheKey(null, pcell1);
        PRangeCellPlus.PCellCacheKey keyOtherPCell = new PRangeCellPlus.PCellCacheKey(null, pcell2);

        Assertions.assertNotEquals(keyNull, keyOtherPCell);
    }

    @Test
    public void testPCellWithNormOf() throws Exception {
        final String partitionName = "p1";
        final PartitionKey key1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey key2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));

        Range<PartitionKey> r1 = Range.closed(key1, key2);
        PRangeCellPlus base = new PRangeCellPlus(partitionName, new PRangeCell(r1));
        PRangeCellPlus normalized = new PRangeCellPlus("p_norm", new PRangeCell(r1));

        PRangeCellPlus.PCellWithNorm norm = PRangeCellPlus.PCellWithNorm.of(base, normalized);
        Assertions.assertEquals(base, norm.basePCell());
        Assertions.assertEquals(normalized, norm.normalized());
    }

    @Test
    public void testNormalizePRangeCellPluss1() throws Exception {
        final String partitionName1 = "p1";
        final String partitionName2 = "p2";
        final PartitionKey k1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey k2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        final PartitionKey k3 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 12, 0, 0));
        final PartitionKey k4 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 13, 0, 0));

        Range<PartitionKey> r1 = Range.closed(k1, k2);
        Range<PartitionKey> r2 = Range.closed(k3, k4);
        PRangeCellPlus p1 = new PRangeCellPlus(partitionName1, new PRangeCell(r1));
        PRangeCellPlus p2 = new PRangeCellPlus(partitionName2, new PRangeCell(r2));

        // Provide a minimal PCellSortedSet by overriding getPartitions()
        PCellSortedSet rangeMap = new PCellSortedSet(new TreeSet<>()) {
            @Override
            public NavigableSet<PRangeCellPlus> getPartitions() {
                return new TreeSet<>(List.of(p1, p2));
            }
        };

        List<PRangeCellPlus.PCellWithNorm> result = PRangeCellPlus.normalizePRangeCellPluss(null, null, rangeMap, null);
        Assertions.assertEquals(2, result.size());
        Assertions.assertSame(result.get(0).basePCell(), result.get(0).normalized());
        Assertions.assertSame(result.get(1).basePCell(), result.get(1).normalized());
    }

    @Test
    public void testNormalizePRangeCellPluss2() throws Exception {
        final String partitionName1 = "p1";
        final PartitionKey k1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey k2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        Range<PartitionKey> r1 = Range.closed(k1, k2);
        PRangeCellPlus p1 = new PRangeCellPlus(partitionName1, new PRangeCell(r1));

        PCellSortedSet rangeMap = new PCellSortedSet(new TreeSet<>()) {
            @Override
            public NavigableSet<PRangeCellPlus> getPartitions() {
                return new TreeSet(List.of(p1));
            }
        };

        Expr slotRef = new SlotRef(new SlotId(9));
        List<PRangeCellPlus.PCellWithNorm> result = PRangeCellPlus.normalizePRangeCellPluss(null, null, rangeMap, slotRef);
        Assertions.assertEquals(1, result.size());
        Assertions.assertSame(result.get(0).basePCell(), result.get(0).normalized());
    }

    @Test
    public void testtoNormalizedCell() throws Exception {
        final String partitionName = "p1";
        final PartitionKey k1 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 10, 0, 0));
        final PartitionKey k2 = PartitionKey.ofDateTime(LocalDateTime.of(2025, 5, 27, 11, 0, 0));
        Range<PartitionKey> r1 = Range.closed(k1, k2);
        PRangeCellPlus pcell = new PRangeCellPlus(partitionName, new PRangeCell(r1));

        Assertions.assertSame(pcell, PRangeCellPlus.toNormalizedCell(pcell, null));
        Assertions.assertSame(pcell, PRangeCellPlus.toNormalizedCell(pcell, new SlotRef(new SlotId(1))));
    }
}
