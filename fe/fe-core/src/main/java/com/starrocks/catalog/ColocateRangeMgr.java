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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Manages colocate ranges for range distribution colocate groups.
 *
 * <p>Each colocate group (identified by colocateGroupId, i.e., GroupId.grpId) maintains
 * an ordered list of {@link ColocateRange} entries that:
 * <ul>
 *   <li>Cover the full value domain [MIN, MAX)</li>
 *   <li>Are non-overlapping, ordered, and contiguous</li>
 *   <li>Each map to a unique PACK shard group ID</li>
 * </ul>
 *
 * <p>This structure is shared across databases: tables in different databases with the
 * same colocateGroupId share the same colocate ranges and PACK shard groups.
 */
public class ColocateRangeMgr {

    @SerializedName("cr")
    private Map<Long, List<ColocateRange>> colocateGroupToRanges = Maps.newHashMap();

    // ---- Query ----

    /**
     * Returns all colocate ranges for the given colocate group.
     *
     * @param colocateGroupId the colocate group id (GroupId.grpId)
     * @return ordered list of ColocateRange, or empty list if group not found
     */
    public List<ColocateRange> getColocateRanges(long colocateGroupId) {
        return colocateGroupToRanges.getOrDefault(colocateGroupId, Collections.emptyList());
    }

    /**
     * Finds the ColocateRange that contains the given colocate value using binary search.
     *
     * <p>Creates a point range [value, value] and uses {@link Collections#binarySearch}.
     * This works because {@link ColocateRange} implements {@code Comparable<Range<Tuple>>}
     * by delegating to {@link Range#compareTo(Range)}, which returns 0 for overlapping ranges.
     *
     * @param colocateGroupId the colocate group id
     * @param colocateValue the colocate column value to look up
     * @return the ColocateRange containing the value, or null if not found
     */
    public ColocateRange getColocateRange(long colocateGroupId, Tuple colocateValue) {
        int index = getColocateRangeIndex(colocateGroupId, colocateValue);
        if (index < 0) {
            return null;
        }
        return colocateGroupToRanges.get(colocateGroupId).get(index);
    }

    /**
     * Returns the index of the ColocateRange that contains the given colocate value.
     *
     * <p>The index is stable across all tables sharing this colocate group: the i-th
     * range in {@link #getColocateRanges(long)} corresponds to the same PACK shard group
     * across every partition/table/database in the group. Coordinator-side scan-range
     * dispatch uses this index as the bucket sequence so that scan ranges from joined
     * tables that share a colocate range are routed to the same fragment instance.
     *
     * @param colocateGroupId the colocate group id
     * @param colocateValue the colocate prefix to look up; null means the global lower
     *                      bound (-inf), which always lands in the first range
     * @return the index in {@code [0, getColocateRanges(grpId).size())}, or -1 if the
     *         group does not exist or the value is not covered
     */
    public int getColocateRangeIndex(long colocateGroupId, Tuple colocateValue) {
        return indexOf(colocateGroupToRanges.get(colocateGroupId), colocateValue);
    }

    /**
     * Binary-search variant of {@link #getColocateRangeIndex} that operates on a caller-supplied
     * range list. Used when the caller is mutating a working copy in-flight (e.g. splice path)
     * and cannot consult the live {@code colocateGroupToRanges} map.
     *
     * @return the index of the {@link ColocateRange} containing {@code value}, or {@code -1}
     *         if {@code ranges} is null/empty or the value is not covered. {@code value == null}
     *         maps to the first range (the global {@code -infinity} sentinel).
     */
    public static int indexOf(List<ColocateRange> ranges, Tuple value) {
        if (ranges == null || ranges.isEmpty()) {
            return -1;
        }
        if (value == null) {
            return 0;
        }
        int index = Collections.binarySearch(ranges, Range.gele(value, value));
        return index >= 0 ? index : -1;
    }

    /**
     * Returns true iff the range list has an entry whose lower bound is exactly {@code prefix}
     * with inclusive lower (i.e. {@code prefix} is already a registered colocate boundary).
     */
    public static boolean hasBoundaryAt(List<ColocateRange> ranges, Tuple prefix) {
        int idx = indexOf(ranges, prefix);
        if (idx < 0) {
            return false;
        }
        Range<Tuple> range = ranges.get(idx).getRange();
        return !range.isMinimum() && range.isLowerBoundIncluded()
                && java.util.Objects.equals(range.getLowerBound(), prefix);
    }

    /**
     * Returns true if the given colocate group exists.
     */
    public boolean containsColocateGroup(long colocateGroupId) {
        return colocateGroupToRanges.containsKey(colocateGroupId);
    }

    // ---- Initialize ----

    /**
     * Initializes a new colocate group with a single range covering the full value domain.
     *
     * @param colocateGroupId the colocate group id
     * @param shardGroupId the PACK shard group id for the initial full range
     */
    public void initColocateGroup(long colocateGroupId, long shardGroupId) {
        List<ColocateRange> ranges = new ArrayList<>();
        ranges.add(new ColocateRange(Range.all(), shardGroupId));
        List<ColocateRange> existing = colocateGroupToRanges.putIfAbsent(colocateGroupId, ranges);
        if (existing != null) {
            throw new IllegalArgumentException("Colocate group " + colocateGroupId + " already exists");
        }
    }

    // ---- Set (for replay) ----

    /**
     * Directly sets the colocate ranges for a group. Used during edit log replay and image loading.
     */
    public void setColocateRanges(long colocateGroupId, List<ColocateRange> ranges) {
        colocateGroupToRanges.put(colocateGroupId, new ArrayList<>(ranges));
    }

    // ---- Remove ----

    /**
     * Removes the colocate group and all its ranges.
     */
    public void removeColocateGroup(long colocateGroupId) {
        colocateGroupToRanges.remove(colocateGroupId);
    }
}
