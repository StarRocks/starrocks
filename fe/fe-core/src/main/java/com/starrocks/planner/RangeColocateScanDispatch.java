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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Scan-time dispatch facade for range distribution colocate tables.
 *
 * <p>Encapsulates the policy that lets {@link OlapScanNode} and
 * {@link com.starrocks.sql.plan.PlanFragmentBuilder} produce range-aware
 * bucket sequences and verify that the colocate group is aligned with the
 * actual tablet layout. The catalog layer ({@link ColocateRangeUtils}) keeps
 * only the pure range-geometry primitives (range expansion / prefix
 * extraction); this class composes those with {@link ColocateRangeMgr} state
 * to drive scan-time decisions.
 *
 * <p>Lifecycle: callers obtain an instance per call via {@link #forTable},
 * which returns {@code null} for tables that are not range colocate
 * participants. Do not cache the instance across phases — the alignment
 * guard must observe the current {@code ColocateRangeMgr} at dispatch time.
 */
public final class RangeColocateScanDispatch {

    private final OlapTable olapTable;
    private final ColocateTableIndex colocateTableIndex;
    private final long colocateGroupId;
    private final int colocateColumnCount;

    private RangeColocateScanDispatch(OlapTable olapTable,
                                       ColocateTableIndex colocateTableIndex,
                                       long colocateGroupId,
                                       int colocateColumnCount) {
        this.olapTable = olapTable;
        this.colocateTableIndex = colocateTableIndex;
        this.colocateGroupId = colocateGroupId;
        this.colocateColumnCount = colocateColumnCount;
    }

    /**
     * Returns a dispatch instance iff {@code olapTable} is a range distribution
     * lake table that participates in a range colocate group, otherwise
     * {@code null}.
     *
     * <p>Resolves {@link ColocateTableIndex} from {@link GlobalStateMgr} once
     * at construction; the same instance is reused by subsequent
     * {@link #bucketCount}, {@link #requireAligned}, and
     * {@link #computeBucketSeq} calls.
     */
    @Nullable
    public static RangeColocateScanDispatch forTable(OlapTable olapTable) {
        if (olapTable.getDefaultDistributionInfo().getType() != DistributionInfo.DistributionInfoType.RANGE) {
            return null;
        }
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (!colocateTableIndex.isColocateTable(olapTable.getId())) {
            return null;
        }
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(olapTable.getId());
        if (groupId == null || !colocateTableIndex.isRangeColocateGroup(groupId)) {
            return null;
        }
        ColocateGroupSchema schema = colocateTableIndex.getGroupSchema(groupId);
        return new RangeColocateScanDispatch(
                olapTable, colocateTableIndex, groupId.grpId, schema.getColocateColumnCount());
    }

    /**
     * Returns the colocate range count, used as the bucket bound that
     * {@link com.starrocks.qe.ColocatedBackendSelector}'s
     * {@code bucketSeq < bucketNum} invariant requires.
     */
    public int bucketCount() {
        int count = colocateTableIndex.getColocateRangeMgr().getColocateRanges(colocateGroupId).size();
        // ColocateRangeMgr always seeds a group with the [MIN, MAX) range,
        // so a registered group has at least one entry.
        Preconditions.checkState(count > 0,
                "range colocate group %s has no colocate ranges", colocateGroupId);
        return count;
    }

    /**
     * Verifies every {@link MaterializedIndex} actually scanned in the supplied
     * physical partitions is aligned with the colocate group. Throws
     * {@link IllegalStateException} on the first misaligned one.
     *
     * <p>Caller invariant: {@code physicalPartitions} must be the partitions
     * that actually contributed scan tablets — sibling sub-partitions that
     * were pruned out must not be passed in.
     */
    public void requireAligned(Iterable<PhysicalPartition> physicalPartitions, long indexMetaId) {
        for (PhysicalPartition physicalPartition : physicalPartitions) {
            MaterializedIndex selectedIndex = physicalPartition.getLatestIndex(indexMetaId);
            if (computeBucketSeq(selectedIndex) == null) {
                throw new IllegalStateException(String.format(
                        "range colocate group %d is in an unaligned state in physical partition %d; "
                                + "cannot dispatch colocate join until alignment is restored",
                        colocateGroupId, physicalPartition.getId()));
            }
        }
    }

    /**
     * Returns the tablet-id → bucket-sequence mapping for {@code selectedIndex}
     * when every tablet's range is contained in exactly one ColocateRange and
     * every ColocateRange has at least one covering tablet (the colocate group
     * is fully aligned).
     *
     * <p>Returns {@code null} when the colocate group is in a transient
     * unaligned state — a spanning tablet, or a missing range. The caller
     * falls back to position-based bucketSeq; {@link #requireAligned} is the
     * dispatch-time correctness guard.
     *
     * <p>Throws {@link IllegalStateException} when a tablet has no
     * {@link com.starrocks.catalog.TabletRange} despite being in a range
     * colocate group — this is a P0/P1 invariant violation, not a transient
     * state.
     */
    @Nullable
    public Map<Long, Integer> computeBucketSeq(MaterializedIndex selectedIndex) {
        ColocateRangeMgr colocateRangeMgr = colocateTableIndex.getColocateRangeMgr();
        List<ColocateRange> colocateRanges = colocateRangeMgr.getColocateRanges(colocateGroupId);
        Preconditions.checkState(!colocateRanges.isEmpty(),
                "range colocate group %s has no colocate ranges", colocateGroupId);
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(olapTable);

        // Pre-expand each ColocateRange once.
        List<Range<Tuple>> expandedRanges = new ArrayList<>(colocateRanges.size());
        for (ColocateRange colocateRange : colocateRanges) {
            expandedRanges.add(ColocateRangeUtils.expandToFullSortKey(
                    colocateRange.getRange(), sortKeyColumns, colocateColumnCount));
        }

        Map<Long, Integer> output = new HashMap<>();
        Set<Integer> coveredBucketSeqs = new HashSet<>();
        for (Long tabletId : selectedIndex.getTabletIdsInOrder()) {
            Tablet tablet = selectedIndex.getTablet(tabletId);
            Preconditions.checkState(tablet.getRange() != null,
                    "tablet %s in range colocate group %s has no TabletRange",
                    tabletId, colocateGroupId);
            Range<Tuple> tabletRange = tablet.getRange().getRange();
            Tuple prefix = ColocateRangeUtils.extractColocatePrefix(tabletRange, colocateColumnCount);
            int bucketSeq = colocateRangeMgr.getColocateRangeIndex(colocateGroupId, prefix);
            if (bucketSeq < 0) {
                return null; // tablet prefix does not match any ColocateRange
            }
            if (!expandedRanges.get(bucketSeq).contains(tabletRange)) {
                return null; // spanning tablet — colocate group is unaligned
            }
            output.put(tabletId, bucketSeq);
            coveredBucketSeqs.add(bucketSeq);
        }
        if (coveredBucketSeqs.size() != colocateRanges.size()) {
            return null; // missing-range — some ColocateRange has no covering tablet
        }
        return output;
    }
}
