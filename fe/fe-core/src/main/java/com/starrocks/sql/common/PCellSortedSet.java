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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A sorted set of PCellWithName with name-based access which is used to unify Range and List partitions.
 * <p>
 * PCellSortedSet can be used to:
 * - get/remove partition by name
 * - get sorted partitions by pCell's natural order
 * <p>
 * NOTE:
 * - Partitions are sorted by PCellWithName's compareTo method which is sorted by
 *      PCell(range or list partition value) first, then sorted by name.
 * - Name-based access is case-insensitive.
 */
public class PCellSortedSet {
    // partitions are sorted by PCellWithName's compareTo method
    private final NavigableSet<PCellWithName> pCellWithNames;
    // cache for name to partition mapping which is used to get by name
    private final Map<String, PCellWithName> nameToPCell = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    public PCellSortedSet(NavigableSet<PCellWithName> partitions) {
        this.pCellWithNames = partitions;
        for (PCellWithName partition : partitions) {
            // ensure no duplicate names
            if (nameToPCell.containsKey(partition.name())) {
                throw new IllegalArgumentException("Duplicate partition name: " + partition.name());
            }
            nameToPCell.put(partition.name(), partition);
        }
    }

    public static PCellSortedSet of(PCellSortedSet other) {
        return new PCellSortedSet(new TreeSet<>(other.pCellWithNames));
    }

    public static PCellSortedSet of() {
        return new PCellSortedSet(new TreeSet<>());
    }

    public static PCellSortedSet of(NavigableSet<PCellWithName> partitions) {
        return new PCellSortedSet(partitions);
    }

    public static PCellSortedSet of(Map<String, PCell> input) {
        NavigableSet<PCellWithName> partitions = input.entrySet()
                .stream()
                .map(entry -> PCellWithName.of(entry.getKey(), entry.getValue()))
                .collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
        return new PCellSortedSet(partitions);
    }

    public static PCellSortedSet of(Collection<PCellWithName> partitions) {
        return new PCellSortedSet(new TreeSet<>(partitions));
    }

    /**
     * Add a partition to the set.
     * @param name the name of the partition
     * @param partition the partition to add
     */
    public void add(String name, PCell partition) {
        add(PCellWithName.of(name, partition));
    }

    /**
     * Add a partition to the set.
     * @param partition the partition to add
     */
    public void add(PCellWithName partition) {
        String name = partition.name();
        PCellWithName existing = nameToPCell.get(name);
        if (existing != null) {
            // If partition already exists and is the same, do nothing
            if (existing.equals(partition)) {
                return;
            }
            // If partition exists but is different, remove the old one first
            pCellWithNames.remove(existing);
        }
        nameToPCell.put(name, partition);
        pCellWithNames.add(partition);
    }

    /**
     * Check if the set is empty.
     * @return true if the set is empty, false otherwise
     */
    public boolean isEmpty() {
        return pCellWithNames.isEmpty();
    }

    /**
     * Get the number of partitions in the set.
     * @return the number of partitions
     */
    public int size() {
        return pCellWithNames.size();
    }

    /**
     * Check if the given partition exists.
     * @param partition the partition to check
     * @return true if the partition exists, false otherwise
     */
    public boolean containsPCellWithName(PCellWithName partition) {
        return pCellWithNames.contains(partition);
    }

    /**
     * Check if a partition with the given name exists.
     * @param partitionName the name of the partition
     * @return true if the partition exists, false otherwise
     */
    public boolean containsName(String partitionName) {
        return nameToPCell.containsKey(partitionName);
    }

    /**
     * Get partition PCellWithName by name.
     * @param partitionName the name of the partition
     * @return the PCellWithName of the partition, or null if not found
     */
    public PCellWithName getPCellWithName(String partitionName) {
        return nameToPCell.get(partitionName);
    }

    /**
     * Get partition PCell by name.
     * @param partitionName the name of the partition
     * @return the PCell of the partition, or null if not found
     */
    public PCell getPCell(String partitionName) {
        PCellWithName pCellWithName = nameToPCell.get(partitionName);
        return pCellWithName != null ? pCellWithName.cell() : null;
    }

    /**
     * Remove partition by name.
     * @param partitionName the name of the partition to remove
     * @return true if the partition was removed, false otherwise
     */
    public boolean removeByName(String partitionName) {
        PCellWithName partition = nameToPCell.remove(partitionName);
        if (partition != null) {
            pCellWithNames.remove(partition);
            return true;
        }
        return false;
    }

    /**
     * Remove the given partition.
     * @param partition the partition to remove
     * @return true if the partition was removed, false otherwise
     */
    public boolean remove(PCellWithName partition) {
        PCellWithName removed = nameToPCell.remove(partition.name());
        boolean removedFromSet = pCellWithNames.remove(partition);
        return removed != null || removedFromSet;
    }

    /**
     * Get the navigable set of partitions.
     */
    public NavigableSet<PCellWithName> getPartitions() {
        return pCellWithNames;
    }

    /**
     * Get a stream of the partitions.
     */
    public Stream<PCellWithName> stream() {
        return pCellWithNames.stream();
    }

    /**
     * Perform the given action for each partition in the set.
     * @param action the action to be performed for each partition
     */
    public void forEach(Consumer<? super PCellWithName> action) {
        pCellWithNames.forEach(action);
    }

    /**
     * Get a descending iterator over the partitions.
     */
    public Iterator<PCellWithName> descendingIterator() {
        return new ConsistentIterator(pCellWithNames.descendingIterator());
    }

    /**
     * Get an iterator over the partitions.
     */
    public Iterator<PCellWithName> iterator() {
        return new ConsistentIterator(pCellWithNames.iterator());
    }

    /**
     * Custom iterator that maintains consistency between partitions and nameToPCell
     * when removing elements.
     */
    private class ConsistentIterator implements Iterator<PCellWithName> {
        private final Iterator<PCellWithName> delegate;
        private PCellWithName current;

        public ConsistentIterator(Iterator<PCellWithName> delegate) {
            this.delegate = delegate;
            this.current = null;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public PCellWithName next() {
            current = delegate.next();
            return current;
        }

        @Override
        public void remove() {
            if (current == null) {
                throw new IllegalStateException("next() must be called before remove()");
            }
            // Remove from the underlying iterator (partitions)
            delegate.remove();
            // Also remove from nameToPCell to maintain consistency
            nameToPCell.remove(current.name());
            current = null;
        }
    }

    /**
     * only reserve the last limit of partitions.
     * @param toReserve number of partitions to reserve
     */
    public void reserveToSize(int toReserve) {
        if (toReserve <= 0 || toReserve >= pCellWithNames.size()) {
            return;
        }
        Iterator<PCellWithName> iterator = iterator();
        int removeCount = pCellWithNames.size() - toReserve;
        int i = 0;
        while (iterator.hasNext() && i++ < removeCount) {
            iterator.next();
            iterator.remove();
        }
    }

    /**
     * Remove partitions that match the given predicate.
     * @param predicate the predicate to test partitions against
     */
    public void removeIf(Predicate<PCellWithName> predicate) {
        Iterator<PCellWithName> iterator = iterator();
        while (iterator.hasNext()) {
            PCellWithName partition = iterator.next();
            if (predicate.test(partition)) {
                iterator.remove();
            }
        }
    }

    /**
     * Remove a number of partitions from the start.
     * @param size number of partitions to remove
     */
    public void removeFromStart(int size) {
        if (size <= 0 || size >= pCellWithNames.size()) {
            return;
        }
        Iterator<PCellWithName> iterator = iterator();
        int i = 0;
        while (iterator.hasNext() && i++ < size) {
            iterator.next();
            iterator.remove();
        }
    }

    /**
     * Get all partition names in the set.
     */
    public Set<String> getPartitionNames() {
        return nameToPCell.keySet();
    }

    /**
     * Add all partitions from another PCellSortedSet.
     */
    public void addAll(PCellSortedSet other) {
        if (other == null || other.isEmpty()) {
            return;
        }
        for (PCellWithName partition : other.pCellWithNames) {
            add(partition);
        }
    }

    @Override
    public String toString() {
        if (pCellWithNames == null || pCellWithNames.isEmpty()) {
            return "";
        }
        int maxLen = Config.max_mv_task_run_meta_message_values_length;
        int size = pCellWithNames.size();

        if (size <= maxLen) {
            // Join all names if under limit - use sorted order from partitions
            return pCellWithNames.stream()
                    .map(PCellWithName::name)
                    .collect(Collectors.joining(","));
        } else {
            int half = maxLen / 2;
            List<String> names = pCellWithNames.stream()
                    .map(PCellWithName::name)
                    .collect(Collectors.toList());

            String prefix = names.stream()
                    .limit(half)
                    .collect(Collectors.joining(","));
            String suffix = names.stream()
                    .skip(size - half)
                    .collect(Collectors.joining(","));

            return prefix + ",...," + suffix;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pCellWithNames);
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
        return pCellWithNames.equals(that.pCellWithNames);
    }
}
