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

import com.google.common.base.Joiner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A sorted set of PCellWithName.
 */
public record PCellSortedSet(SortedSet<PCellWithName> partitions) {

    public static PCellSortedSet of() {
        return new PCellSortedSet(new TreeSet<>());
    }

    public static PCellSortedSet of(SortedSet<PCellWithName> partitions) {
        return new PCellSortedSet(partitions);
    }

    public static PCellSortedSet of(Map<String, ? extends PCell> input) {
        SortedSet<PCellWithName> partitions = input.entrySet()
                .stream()
                .map(entry -> PCellWithName.of(entry.getKey(), entry.getValue()))
                .collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
        return new PCellSortedSet(partitions);
    }

    public static PCellSortedSet of(List<PCellWithName> input) {
        SortedSet<PCellWithName> partitions = new TreeSet<>(input);
        return new PCellSortedSet(partitions);
    }

    public static PCellSortedSet of(PCellSortedSet other) {
        if (other == null || other.isEmpty()) {
            return new PCellSortedSet(new TreeSet<>());
        }
        return new PCellSortedSet(new TreeSet<>(other.partitions));
    }

    public void add(PCellWithName partition) {
        partitions.add(partition);
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    public int size() {
        return partitions.size();
    }

    public boolean contains(PCellWithName partition) {
        return partitions.contains(partition);
    }

    public boolean remove(PCellWithName partition) {
        return partitions.remove(partition);
    }

    public SortedSet<PCellWithName> getPartitions() {
        return partitions;
    }

    public Stream<PCellWithName> stream() {
        return partitions.stream();
    }

    public void forEach(Consumer<? super PCellWithName> action) {
        partitions.forEach(action);
    }

    public Iterator<PCellWithName> iterator() {
        return partitions.iterator();
    }

    public PCellSortedSet skip(int skip) {
        if (skip <= 0) {
            return this;
        }
        if (skip >= partitions.size()) {
            return new PCellSortedSet(new TreeSet<>());
        }
        SortedSet<PCellWithName> skippedPartitions = new TreeSet<>(partitions);
        Iterator<PCellWithName> iterator = skippedPartitions.iterator();
        for (int i = 0; i < skip && iterator.hasNext(); i++) {
            iterator.next();
            iterator.remove();
        }
        return new PCellSortedSet(skippedPartitions);
    }

    /**
     * only reserve the last limit of partitions.
     * @param limit
     * @return
     */
    public PCellSortedSet limit(int limit) {
        if (limit <= 0 || limit >= partitions.size()) {
            return this;
        }
        SortedSet<PCellWithName> limitedPartitions = new TreeSet<>(partitions);
        Iterator<PCellWithName> iterator = limitedPartitions.iterator();
        int count = 0;
        while (iterator.hasNext() && count < limitedPartitions.size() - limit) {
            iterator.next();
            iterator.remove();
            count++;
        }
        return new PCellSortedSet(limitedPartitions);
    }

    public void removeWithSize(int size) {
        if (size <= 0 || size >= partitions.size()) {
            return;
        }
        Iterator<PCellWithName> iterator = partitions.iterator();
        int count = 0;
        while (iterator.hasNext() && count < size) {
            iterator.next();
            iterator.remove();
            count++;
        }
    }

    public Set<String> getPartitionNames() {
        return partitions.stream().map(PCellWithName::name).collect(java.util.stream.Collectors.toSet());
    }

    public void addAll(PCellSortedSet other) {
        if (other == null || other.isEmpty()) {
            return;
        }
        partitions.addAll(other.partitions);
    }

    public Map<String, PCell> toCellMap() {
        return partitions.stream().collect(Collectors.toMap(PCellWithName::name, PCellWithName::cell));
    }

    @Override
    public String toString() {
        if (partitions == null || partitions.isEmpty()) {
            return "";
        }
        return Joiner.on(",").join(getPartitionNames());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PCellSortedSet)) {
            return false;
        }
        PCellSortedSet that = (PCellSortedSet) o;
        return partitions.equals(that.partitions);
    }
}
