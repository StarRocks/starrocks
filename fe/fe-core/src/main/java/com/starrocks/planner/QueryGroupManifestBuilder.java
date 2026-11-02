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

import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.QueryGroupManifest.Group;
import com.starrocks.planner.QueryGroupManifest.KeyDefinition;
import com.starrocks.planner.QueryGroupManifest.Source;
import com.starrocks.planner.QueryGroupManifest.Task;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Pure geometry builder: no metadata refresh, predicate pruning, scheduling or invented cuts. */
public final class QueryGroupManifestBuilder {
    public static final int MAX_GROUPS = 1024;
    public static final int MAX_TASKS = 8192;

    private QueryGroupManifestBuilder() {
    }

    public static QueryGroupManifest build(int scanNodeId, KeyDefinition keys, List<Source> capturedSources)
            throws StarRocksException {
        require(capturedSources != null && !capturedSources.isEmpty(), "no captured sources");
        // Every source needs at least one task. Reject before inspecting or copying a large input.
        require(capturedSources.size() <= MAX_TASKS, "source count exceeds 8192 task limit");
        require(scanNodeId >= 0, "invalid scan node ID");
        require(keys != null && keys.tableId() > 0 && keys.indexMetaId() > 0, "invalid table/index identity");
        require(keys.orderedCUniqueIds() != null && keys.orderedCUniqueIds().size() == 3
                && new HashSet<>(keys.orderedCUniqueIds()).size() == 3
                && keys.orderedCUniqueIds().stream().allMatch(id -> id >= 0),
                "expected three distinct ordered C unique IDs");
        List<Source> sources = new ArrayList<>(capturedSources);
        validateSources(sources);
        sources.sort(Comparator.comparingInt(Source::sourceIndex));
        List<Range<Tuple>> intervals = buildIntervals(sources);
        require(intervals.size() >= 2, "fewer than two geometrically nonempty C groups; use colocate");

        List<List<Task>> groupTasks = new ArrayList<>();
        for (int i = 0; i < intervals.size(); i++) {
            groupTasks.add(new ArrayList<>());
        }
        long[] weights = new long[intervals.size()];
        List<Task> tasks = new ArrayList<>();
        for (Source source : sources) {
            Range<Tuple> range = source.tabletRange();
            int first = firstOverlapping(intervals, range);
            int end = first;
            // Walk only overlapping atoms; stop before allocating any task beyond the budget.
            while (end < intervals.size() && range.isOverlapping(intervals.get(end))) {
                require(tasks.size() + end - first < MAX_TASKS, "clip task count exceeds 8192 limit");
                end++;
            }
            require(end > first, "captured source has no group coverage");
            long cost = source.estimatedBytes() > 0 ? source.estimatedBytes()
                    : source.estimatedRows() > 0 ? source.estimatedRows() : 1;
            int intersections = end - first;
            long quotient = cost / intersections;
            long remainder = cost % intersections;
            for (int i = first; i < end; i++) {
                // Every source endpoint is an atom boundary, so the overlapping atom is the exact clip.
                Task task = new Task(source.sourceIndex(), i, intervals.get(i));
                tasks.add(task);
                groupTasks.get(i).add(task);
                long weight = Math.max(1, quotient + (i - first < remainder ? 1 : 0));
                weights[i] = saturatedAdd(weights[i], weight);
            }
        }
        List<Group> groups = new ArrayList<>();
        for (int i = 0; i < intervals.size(); i++) {
            groups.add(new Group(i, intervals.get(i), groupTasks.get(i), weights[i]));
        }
        return new QueryGroupManifest(scanNodeId, keys, sources, groups, tasks);
    }

    private static void validateSources(List<Source> sources) throws StarRocksException {
        Set<Integer> indices = new HashSet<>();
        Set<Long> tablets = new HashSet<>();
        Map<Long, List<Range<Tuple>>> partitions = new HashMap<>();
        for (Source source : sources) {
            require(source != null, "missing captured source");
            require(source.sourceIndex() >= 0 && source.physicalPartitionId() > 0 && source.tabletId() > 0
                    && source.capturedVersion() >= 0, "invalid captured source identity");
            require(indices.add(source.sourceIndex()), "duplicate source index");
            require(tablets.add(source.tabletId()), "duplicate tablet capture or conflicting version/schema");
            TScanRangeLocations original = source.original();
            require(original != null && original.isSetScan_range()
                    && original.getScan_range().isSetInternal_scan_range(), "missing captured internal scan range");
            require(!original.getScan_range().isSetGrouped_range_task(), "source already contains a grouped task");
            TInternalScanRange internal = original.getScan_range().getInternal_scan_range();
            require(internal.isSetTablet_id() && internal.getTablet_id() == source.tabletId()
                    && internal.isSetPartition_id() && internal.getPartition_id() == source.physicalPartitionId()
                    && internal.isSetVersion() && Long.toString(source.capturedVersion()).equals(internal.getVersion())
                    && internal.isSetSchema_hash() && Integer.toString(source.schemaHash()).equals(internal.getSchema_hash()),
                    "captured internal source identity/version/schema mismatch");
            Range<Tuple> range = source.tabletRange();
            validateRange(range);
            partitions.computeIfAbsent(source.physicalPartitionId(), ignored -> new ArrayList<>()).add(range);
        }
        require(partitions.size() >= 2, "fewer than two selected physical partitions; use colocate");
        for (List<Range<Tuple>> ranges : partitions.values()) {
            ranges.sort(Comparator.comparing(Range::getLowerBound, Comparator.nullsFirst(Comparator.naturalOrder())));
            for (int i = 1; i < ranges.size(); i++) {
                require(!ranges.get(i - 1).isOverlapping(ranges.get(i)), "overlapping captured physical sources");
            }
        }
    }

    private static void validateRange(Range<Tuple> range) throws StarRocksException {
        require(range != null, "missing captured tablet range");
        require((range.isMinimum() || range.isLowerBoundIncluded()) && !range.isUpperBoundIncluded(),
                "expected half-open captured tablet range");
        validateTuple(range.getLowerBound());
        validateTuple(range.getUpperBound());
        require(range.isMinimum() || range.isMaximum()
                || range.getLowerBound().compareTo(range.getUpperBound()) < 0, "empty or reversed captured tablet range");
    }

    private static void validateTuple(Tuple tuple) throws StarRocksException {
        if (tuple == null) {
            return;
        }
        require(tuple.getValues().size() == 3, "expected full three-column C tuple");
        for (Variant value : tuple.getValues()) {
            require(value instanceof IntVariant && value.getType().isBigint(), "expected finite NORMAL BIGINT C bounds");
        }
    }

    private static List<Range<Tuple>> buildIntervals(List<Source> sources) throws StarRocksException {
        TreeMap<Tuple, Integer> events = new TreeMap<>();
        int active = 0;
        for (Source source : sources) {
            Range<Tuple> range = source.tabletRange();
            if (range.isMinimum()) {
                active++;
            } else {
                events.merge(range.getLowerBound(), 1, Integer::sum);
            }
            if (!range.isMaximum()) {
                events.merge(range.getUpperBound(), -1, Integer::sum);
            }
        }
        List<Range<Tuple>> intervals = new ArrayList<>();
        Tuple lower = null;
        for (Map.Entry<Tuple, Integer> event : events.entrySet()) {
            if (active > 0) {
                addInterval(intervals, lower, event.getKey());
            }
            active += event.getValue();
            lower = event.getKey();
        }
        if (active > 0) {
            addInterval(intervals, lower, null);
        }
        return intervals;
    }

    private static void addInterval(List<Range<Tuple>> intervals, Tuple lower, Tuple upper)
            throws StarRocksException {
        require(intervals.size() < MAX_GROUPS, "C group count exceeds 1024 limit");
        intervals.add(Range.of(lower, upper, lower != null, false));
    }

    private static int firstOverlapping(List<Range<Tuple>> intervals, Range<Tuple> source) {
        int low = 0;
        int high = intervals.size();
        while (low < high) {
            int middle = low + (high - low) / 2;
            if (intervals.get(middle).isLessThan(source)) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static void require(boolean condition, String reason) throws StarRocksException {
        if (!condition) {
            throw new StarRocksException("range_scan_execution_mode=grouped: " + reason);
        }
    }
}
