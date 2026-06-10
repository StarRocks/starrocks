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

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDistributionType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RangeDistributionSpec}.
 * Covers both structural behavior (equals/hashCode/toString/toThrift) and
 * the PR-3 set-based cross-type {@link RangeDistributionSpec#isSatisfy}
 * and {@link RangeDistributionSpec#canColocate} logic.
 */
class RangeDistributionSpecTest {

    private static RangeDistributionSpec build(long tableId, DistributionCol... cols) {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(tableId, List.of(1L));
        List<DistributionCol> colocateColumns = List.of(cols);
        descriptor.initDistributionUnionFind(colocateColumns);
        return new RangeDistributionSpec(colocateColumns, descriptor);
    }

    private static RangeDistributionSpec buildEmptyPartitions(long tableId, DistributionCol... cols) {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(tableId, Collections.emptyList());
        List<DistributionCol> colocateColumns = List.of(cols);
        descriptor.initDistributionUnionFind(colocateColumns);
        return new RangeDistributionSpec(colocateColumns, descriptor);
    }

    private static HashDistributionSpec hashShuffleJoin(int... colIds) {
        List<DistributionCol> cols = java.util.Arrays.stream(colIds)
                .mapToObj(id -> new DistributionCol(id, true))
                .collect(java.util.stream.Collectors.toList());
        HashDistributionDesc desc = new HashDistributionDesc(cols, HashDistributionDesc.SourceType.SHUFFLE_JOIN);
        return new HashDistributionSpec(desc);
    }

    @Test
    void emptyColocateColumnsRejected() {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(1L, Collections.emptyList());
        assertThrows(IllegalArgumentException.class,
                () -> new RangeDistributionSpec(Collections.emptyList(), descriptor));
    }

    @Test
    void distributionTypeIsRange() {
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true));
        assertEquals(DistributionSpec.DistributionType.RANGE_LOCAL, spec.getType());
    }

    @Test
    void isSatisfyAny() {
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true));
        assertTrue(spec.isSatisfy(AnyDistributionSpec.INSTANCE));
    }

    @Test
    void isSatisfyRejectsOtherDistributionTypes() {
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true));
        assertFalse(spec.isSatisfy(new RoundRobinDistributionSpec()));
        assertFalse(spec.isSatisfy(new ReplicatedDistributionSpec()));
        assertFalse(spec.isSatisfy(new GatherDistributionSpec()));
    }

    @Test
    void isSatisfyRejectsHashLocalAndShuffleAgg() {
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true));
        List<DistributionCol> cols = List.of(new DistributionCol(1, true));
        HashDistributionSpec localHash = new HashDistributionSpec(
                new HashDistributionDesc(cols, HashDistributionDesc.SourceType.LOCAL));
        HashDistributionSpec aggShuffle = new HashDistributionSpec(
                new HashDistributionDesc(cols, HashDistributionDesc.SourceType.SHUFFLE_AGG));
        HashDistributionSpec enforceShuffle = new HashDistributionSpec(
                new HashDistributionDesc(cols, HashDistributionDesc.SourceType.SHUFFLE_ENFORCE));
        assertFalse(spec.isSatisfy(localHash));
        assertFalse(spec.isSatisfy(aggShuffle));
        assertFalse(spec.isSatisfy(enforceShuffle));
    }

    @Test
    void isSatisfyHashShuffleJoinCoverage() {
        // Colocate cols = [1, 2]; required shuffle covers [1, 2] exactly.
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true), new DistributionCol(2, true));
        assertTrue(spec.isSatisfy(hashShuffleJoin(1, 2)));
        // Permuted required order is OK (set-based match).
        assertTrue(spec.isSatisfy(hashShuffleJoin(2, 1)));
        // Extra required columns beyond the colocate set are OK.
        assertTrue(spec.isSatisfy(hashShuffleJoin(1, 2, 3)));
    }

    @Test
    void isSatisfyHashShuffleJoinPartialCoverageRejected() {
        // Colocate cols = [1, 2]; required only covers [1] → unsafe (GROUP BY a against colocate (a, b)).
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true), new DistributionCol(2, true));
        assertFalse(spec.isSatisfy(hashShuffleJoin(1)));
    }

    @Test
    void canColocateSameGroupStable(@Mocked GlobalStateMgr globalStateMgr,
                                    @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = false;
            }
        };
        RangeDistributionSpec left = build(100L, new DistributionCol(1, true));
        RangeDistributionSpec right = build(200L, new DistributionCol(2, true));
        assertTrue(left.canColocate(right));
    }

    @Test
    void canColocateRejectsDifferentGroup(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked ColocateTableIndex colocateTableIndex) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = false;
            }
        };
        RangeDistributionSpec left = build(100L, new DistributionCol(1, true));
        RangeDistributionSpec right = build(200L, new DistributionCol(2, true));
        assertFalse(left.canColocate(right));
    }

    @Test
    void canColocateRejectsUnstableGroup(@Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = true;
            }
        };
        RangeDistributionSpec left = build(100L, new DistributionCol(1, true));
        RangeDistributionSpec right = build(200L, new DistributionCol(2, true));
        assertFalse(left.canColocate(right));
    }

    @Test
    void canColocateRejectsEmptyPartition(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = false;
            }
        };
        RangeDistributionSpec left = buildEmptyPartitions(100L, new DistributionCol(1, true));
        RangeDistributionSpec right = build(200L, new DistributionCol(2, true));
        assertFalse(left.canColocate(right));
    }

    @Test
    void equalsAndHashCodeBasedOnTableAndColumns() {
        RangeDistributionSpec a = build(100L, new DistributionCol(1, true), new DistributionCol(2, true));
        RangeDistributionSpec b = build(100L, new DistributionCol(1, true), new DistributionCol(2, true));
        RangeDistributionSpec differentTable = build(999L, new DistributionCol(1, true), new DistributionCol(2, true));
        RangeDistributionSpec differentCols = build(100L, new DistributionCol(3, true), new DistributionCol(4, true));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, differentTable);
        assertNotEquals(a, differentCols);
        assertNotEquals(a, new RoundRobinDistributionSpec());
    }

    @Test
    void toStringIncludesColocateColumns() {
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true));
        assertTrue(spec.toString().startsWith("RANGE("));
    }

    @Test
    void toThriftRangeThrows() {
        assertThrows(IllegalStateException.class,
                () -> DistributionSpec.DistributionType.RANGE_LOCAL.toThrift());
    }

    @Test
    void toThriftRoundRobinFallsBackToGather() {
        assertEquals(TDistributionType.GATHER,
                DistributionSpec.DistributionType.ROUND_ROBIN.toThrift());
    }

    @Test
    void toThriftKnownTypesUnchanged() {
        assertEquals(TDistributionType.ANY,
                DistributionSpec.DistributionType.ANY.toThrift());
        assertEquals(TDistributionType.BROADCAST,
                DistributionSpec.DistributionType.BROADCAST.toThrift());
        assertEquals(TDistributionType.SHUFFLE,
                DistributionSpec.DistributionType.SHUFFLE.toThrift());
        assertEquals(TDistributionType.GATHER,
                DistributionSpec.DistributionType.GATHER.toThrift());
    }

    @Test
    void equalsIdentityShortCircuit() {
        // Exercise the `this == obj` branch of equals.
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true));
        assertEquals(spec, spec);
    }

    @Test
    void isSatisfyRangeDispatchesToCanColocate(@Mocked GlobalStateMgr globalStateMgr,
                                                @Mocked ColocateTableIndex colocateTableIndex) {
        // Exercise the `spec instanceof RangeDistributionSpec` branch of isSatisfy
        // via mock of canColocate's dependencies. Different groups → false.
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = false;
            }
        };
        RangeDistributionSpec left = build(100L, new DistributionCol(1, true));
        RangeDistributionSpec right = build(200L, new DistributionCol(2, true));
        assertFalse(left.isSatisfy(right));
    }

    @Test
    void canColocateRejectsNullGroupId(@Mocked GlobalStateMgr globalStateMgr,
                                       @Mocked ColocateTableIndex colocateTableIndex) {
        // isSameGroup returns true but getGroup returns null (metadata inconsistency).
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = null;
                colocateTableIndex.getGroup(200L);
                result = null;
            }
        };
        RangeDistributionSpec left = build(100L, new DistributionCol(1, true));
        RangeDistributionSpec right = build(200L, new DistributionCol(2, true));
        assertFalse(left.canColocate(right));
    }

    @Test
    void getNullRelaxSpecProducesNullRelaxColumns() {
        RangeDistributionSpec spec = build(100L, new DistributionCol(1, true), new DistributionCol(2, true));
        EquivalentDescriptor newDesc = new EquivalentDescriptor(100L, List.of(1L));
        RangeDistributionSpec relaxed = spec.getNullRelaxSpec(newDesc);
        assertEquals(spec.getColocateColumns().size(), relaxed.getColocateColumns().size());
        for (DistributionCol col : relaxed.getColocateColumns()) {
            assertFalse(col.isNullStrict(), "getNullRelaxSpec must produce nullStrict=false columns");
        }
    }
}
