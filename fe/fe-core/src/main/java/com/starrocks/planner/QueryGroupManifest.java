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

import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.thrift.TGroupedRangeTask;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Captured source snapshots and complete-C geometry for one scan in one query attempt. */
public final class QueryGroupManifest {
    public record KeyDefinition(long tableId, long indexMetaId, List<Integer> orderedCUniqueIds) {
        public KeyDefinition {
            orderedCUniqueIds = orderedCUniqueIds == null ? null : List.copyOf(orderedCUniqueIds);
        }
    }

    public record Source(int sourceIndex, long physicalPartitionId, long tabletId, long capturedVersion,
                         int schemaHash, Range<Tuple> tabletRange, TScanRangeLocations original,
                         long estimatedBytes, long estimatedRows) {
        public Source {
            tabletRange = copyRange(tabletRange);
            original = original == null ? null : original.deepCopy();
        }

        @Override
        public Range<Tuple> tabletRange() {
            return copyRange(tabletRange);
        }

        @Override
        public TScanRangeLocations original() {
            return original == null ? null : original.deepCopy();
        }
    }

    public record Task(int sourceIndex, int groupId, Range<Tuple> clip) {
        public Task {
            clip = copyRange(clip);
        }

        @Override
        public Range<Tuple> clip() {
            return copyRange(clip);
        }
    }

    public record Group(int groupId, Range<Tuple> interval, List<Task> tasks, long estimatedWeight) {
        public Group {
            interval = copyRange(interval);
            tasks = List.copyOf(tasks);
            if (new HashSet<>(tasks).size() != tasks.size()) {
                throw new IllegalArgumentException("Duplicate grouped scan task");
            }
        }

        @Override
        public Range<Tuple> interval() {
            return copyRange(interval);
        }
    }

    private final int scanNodeId;
    private final KeyDefinition keys;
    private final List<Source> sources;
    private final List<Group> groups;
    private final List<Task> tasks;
    private final Map<Integer, Source> sourceByIndex;
    private final Map<Task, Task> canonicalTasks;

    // Only the pure builder constructs a manifest, after proving source identity and exact tiling.
    QueryGroupManifest(int scanNodeId, KeyDefinition keys, List<Source> sources, List<Group> groups,
                       List<Task> tasks) {
        this.scanNodeId = scanNodeId;
        this.keys = keys;
        this.sources = List.copyOf(sources);
        this.groups = List.copyOf(groups);
        this.tasks = List.copyOf(tasks);
        this.sourceByIndex = sources.stream().collect(Collectors.toUnmodifiableMap(
                Source::sourceIndex, Function.identity()));
        // Task owns its copied geometry and exposes only copies, so its equality/hash stay stable.
        this.canonicalTasks = groups.stream().flatMap(group -> group.tasks().stream())
                .collect(Collectors.toUnmodifiableMap(Function.identity(), Function.identity()));
    }

    public int getScanNodeId() {
        return scanNodeId;
    }

    public KeyDefinition getKeys() {
        return keys;
    }

    public List<Source> getSources() {
        return sources;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    /** Unique real source tablets; counts describe selected geometry, never predicate-matching rows. */
    public int getSourceCount() {
        return sources.size();
    }

    public int getGroupCount() {
        return groups.size();
    }

    public int getTaskCount() {
        return tasks.size();
    }

    /** Returns an independent descriptor, preserving the captured version, schema and source metadata. */
    public TScanRange toScanRange(Task task) {
        if (task == null || task.groupId() < 0 || task.groupId() >= groups.size()) {
            throw new IllegalArgumentException("Task is not part of this grouped scan manifest");
        }
        Task canonical = canonicalTasks.get(task);
        if (canonical == null) {
            throw new IllegalArgumentException("Task is not part of this grouped scan manifest");
        }
        // Tuple equality can ignore integer types; only the stored task has validated BIGINT bounds.
        Source source = sourceByIndex.get(canonical.sourceIndex());
        TScanRange result = source.original().getScan_range();
        result.getInternal_scan_range().setBucket_sequence(canonical.groupId());
        TGroupedRangeTask grouped = new TGroupedRangeTask();
        grouped.setGroup_id(canonical.groupId());
        grouped.setKey_range(new TabletRange(canonical.clip()).toThrift());
        grouped.setSort_key_unique_ids(new ArrayList<>(keys.orderedCUniqueIds()));
        result.setGrouped_range_task(grouped);
        return result;
    }

    static Range<Tuple> copyRange(Range<Tuple> range) {
        if (range == null) {
            return null;
        }
        return Range.of(copyTuple(range.getLowerBound()), copyTuple(range.getUpperBound()),
                range.isLowerBoundIncluded(), range.isUpperBoundIncluded());
    }

    private static Tuple copyTuple(Tuple tuple) {
        return tuple == null ? null : Tuple.fromThrift(tuple.toThrift());
    }
}
