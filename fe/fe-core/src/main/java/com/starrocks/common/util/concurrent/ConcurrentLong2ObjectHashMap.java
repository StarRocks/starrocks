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

package com.starrocks.common.util.concurrent;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A segmented, thread-safe map that stores long keys, backed by {@link Long2ObjectOpenHashMap} to avoid auto-boxing overhead.
 * <p>
 * <b>Concurrency Model:</b>
 * This implementation uses a two-level locking strategy:
 * <ol>
 *     <li><b>Global Table Lock:</b> A {@link ReentrantReadWriteLock} guards the {@code segments} array structure.
 *         Most operations acquire the <i>read lock</i>. Resizing acquires the <i>write lock</i>,
 *         which blocks all other operations (Stop-The-World).</li>
 *     <li><b>Segment Lock:</b> Each segment is guarded by its own {@link ReentrantReadWriteLock}.
 *         Operations within a segment acquire the appropriate lock (read or write),
 *         allowing concurrent access to different segments.</li>
 * </ol>
 * <p>
 * <b>Performance Characteristics:</b>
 * <ul>
 *     <li><b>Reads:</b> Highly concurrent. Readers only block writers to the same segment (and global resizing).</li>
 *     <li><b>Writes:</b> Concurrent for different segments.</li>
 *     <li><b>Resizing:</b> Expensive and blocking. It is recommended to initialize with an appropriate
 *     {@code segmentCount} and {@code expectedSegmentSize}
 *         to minimize resizing if the data size is known or predictable.</li>
 * </ul>
 * <p>
 * <b>Limitations & Constraints:</b>
 * <ul>
 *     <li><b>Null Values:</b> Null values are <b>not allowed</b> (to ensure accurate size tracking
 *     and {@code get} semantics).</li>
 *     <li><b>Map Views:</b> {@link #keySet()} and {@link #values()} return <b>snapshots</b> (copies)
 *     of the data at the time of call.
 *         They are weakly consistent and do not reflect subsequent changes.</li>
 * </ul>
 *
 * @param <V> value type stored in the map
 */
public class ConcurrentLong2ObjectHashMap<V> {
    private static final int DEFAULT_SEGMENT_COUNT = 16;
    private static final int DEFAULT_SEGMENT_INIT_CAPACITY = 4;
    private static final float DEFAULT_MAX_LOAD_FACTOR = 4.0f;

    private final ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock();
    private final LongAdder size = new LongAdder();
    private final int expectedSegmentSize;
    private final float maxLoadFactor;
    private final int initialSegmentCount;

    private volatile Segment<V>[] segments;
    private volatile int segmentMask;
    private volatile long resizeThreshold;

    public ConcurrentLong2ObjectHashMap() {
        this(DEFAULT_SEGMENT_COUNT, DEFAULT_SEGMENT_INIT_CAPACITY, DEFAULT_MAX_LOAD_FACTOR);
    }

    public ConcurrentLong2ObjectHashMap(int segmentCount, int expectedSegmentSize, float maxLoadFactor) {
        Preconditions.checkArgument(segmentCount > 0, "segment count must be positive");
        Preconditions.checkArgument(expectedSegmentSize >= 0, "expected segment size must be non-negative");
        Preconditions.checkArgument(maxLoadFactor > 0, "maxLoadFactor must be positive");
        this.expectedSegmentSize = expectedSegmentSize;
        this.maxLoadFactor = maxLoadFactor;
        int actualSegmentCount = ceilingNextPowerOfTwo(segmentCount);
        this.initialSegmentCount = actualSegmentCount;
        initializeSegments(actualSegmentCount);
    }

    public V get(long key) {
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock readLock = segment.lock.readLock();
            readLock.lock();
            try {
                return segment.map.get(key);
            } finally {
                readLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public V getOrDefault(long key, V defaultValue) {
        V value = get(key);
        return value != null ? value : defaultValue;
    }

    public boolean containsKey(long key) {
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock readLock = segment.lock.readLock();
            readLock.lock();
            try {
                return segment.map.containsKey(key);
            } finally {
                readLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
    }

    public V put(long key, V value) {
        Preconditions.checkNotNull(value);
        boolean inserted = false;
        V previous;
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock writeLock = segment.lock.writeLock();
            writeLock.lock();
            try {
                previous = segment.map.put(key, value);
                if (previous == null) {
                    inserted = true;
                    size.increment();
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
        if (inserted) {
            maybeResize();
        }
        return previous;
    }

    public V putIfAbsent(long key, V value) {
        Preconditions.checkNotNull(value);
        boolean inserted = false;
        V existing;
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock writeLock = segment.lock.writeLock();
            writeLock.lock();
            try {
                existing = segment.map.get(key);
                if (existing == null) {
                    segment.map.put(key, value);
                    inserted = true;
                    size.increment();
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
        if (inserted) {
            maybeResize();
        }
        return existing;
    }

    public V computeIfAbsent(long key, Function<? super Long, ? extends V> mappingFunction) {
        Preconditions.checkNotNull(mappingFunction, "mappingFunction is null");
        boolean inserted = false;
        V value;
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock writeLock = segment.lock.writeLock();
            writeLock.lock();
            try {
                value = segment.map.get(key);
                if (value == null) {
                    value = mappingFunction.apply(key);
                    if (value != null) {
                        segment.map.put(key, value);
                        inserted = true;
                        size.increment();
                    }
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
        if (inserted) {
            maybeResize();
        }
        return value;
    }

    public V computeIfPresent(long key, BiFunction<? super Long, ? super V, ? extends V> remappingFunction) {
        Preconditions.checkNotNull(remappingFunction, "remappingFunction is null");
        V newValue = null;
        boolean removed = false;
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock writeLock = segment.lock.writeLock();
            writeLock.lock();
            try {
                V oldValue = segment.map.get(key);
                if (oldValue != null) {
                    newValue = remappingFunction.apply(key, oldValue);
                    if (newValue != null) {
                        segment.map.put(key, newValue);
                    } else {
                        segment.map.remove(key);
                        size.decrement();
                        removed = true;
                    }
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
        return removed ? null : newValue;
    }

    public V remove(long key) {
        V removed;
        tableLock.readLock().lock();
        try {
            Segment<V> segment = segmentFor(key);
            Lock writeLock = segment.lock.writeLock();
            writeLock.lock();
            try {
                removed = segment.map.remove(key);
                if (removed != null) {
                    size.decrement();
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            tableLock.readLock().unlock();
        }
        return removed;
    }

    public void clear() {
        tableLock.writeLock().lock();
        try {
            initializeSegments(initialSegmentCount);
            size.reset();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public int size() {
        return (int) Math.min(Integer.MAX_VALUE, size.sum());
    }

    public boolean isEmpty() {
        return size.sum() == 0;
    }

    public Set<Long> keySet() {
        tableLock.readLock().lock();
        try {
            int expectedSize = (int) Math.min(Integer.MAX_VALUE, size.sum());
            Set<Long> snapshot = new HashSet<>(expectedSize);
            for (Segment<V> segment : segments) {
                Lock readLock = segment.lock.readLock();
                readLock.lock();
                try {
                    snapshot.addAll(segment.map.keySet());
                } finally {
                    readLock.unlock();
                }
            }
            return snapshot;
        } finally {
            tableLock.readLock().unlock();
        }
    }

    /**
     * Returns a snapshot of values stored in the map. The returned collection is not backed by the map.
     */
    public Collection<V> values() {
        tableLock.readLock().lock();
        try {
            int expectedSize = (int) Math.min(Integer.MAX_VALUE, size.sum());
            List<V> snapshot = new ArrayList<>(expectedSize);
            for (Segment<V> segment : segments) {
                Lock readLock = segment.lock.readLock();
                readLock.lock();
                try {
                    snapshot.addAll(segment.map.values());
                } finally {
                    readLock.unlock();
                }
            }
            return snapshot;
        } finally {
            tableLock.readLock().unlock();
        }
    }

    private Segment<V> segmentFor(long key) {
        return segments[spread(Long.hashCode(key)) & segmentMask];
    }

    private void maybeResize() {
        if (size.sum() <= resizeThreshold) {
            return;
        }
        tableLock.writeLock().lock();
        try {
            if (size.sum() <= resizeThreshold) {
                return;
            }
            resizeInternal();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    private void resizeInternal() {
        Segment<V>[] oldSegments = this.segments;
        int newSegmentCount = oldSegments.length << 1;
        if (newSegmentCount <= 0) {
            return;
        }

        // Calculate optimized initial capacity for new segments to avoid internal rehashing
        long totalSize = size.sum();
        // We use a load factor of 0.75f which is the default for fastutil maps
        int expectedItemsPerSegment = (int) (totalSize / newSegmentCount);
        int initialCapacity = Math.max(expectedSegmentSize, (int) (expectedItemsPerSegment / 0.75f) + 1);

        Segment<V>[] newSegments = createSegments(newSegmentCount, initialCapacity);
        int newMask = newSegmentCount - 1;

        for (Segment<V> oldSegment : oldSegments) {
            Lock readLock = oldSegment.lock.readLock();
            readLock.lock();
            try {
                for (Long2ObjectMap.Entry<V> entry : oldSegment.map.long2ObjectEntrySet()) {
                    Segment<V> target = newSegments[spread(Long.hashCode(entry.getLongKey())) & newMask];
                    target.map.put(entry.getLongKey(), entry.getValue());
                }
            } finally {
                readLock.unlock();
            }
        }

        this.segments = newSegments;
        this.segmentMask = newMask;
        this.resizeThreshold = calculateResizeThreshold(newSegmentCount);
    }

    private void initializeSegments(int segmentCount) {
        this.segments = createSegments(segmentCount, expectedSegmentSize);
        this.segmentMask = segmentCount - 1;
        this.resizeThreshold = calculateResizeThreshold(segmentCount);
    }

    @SuppressWarnings("unchecked")
    private Segment<V>[] createSegments(int count, int segmentCapacity) {
        Segment<V>[] newSegments = (Segment<V>[]) new Segment[count];
        for (int i = 0; i < count; i++) {
            newSegments[i] = new Segment<>(segmentCapacity);
        }
        return newSegments;
    }

    private long calculateResizeThreshold(int segmentCount) {
        return Math.max(1L, (long) (segmentCount * maxLoadFactor));
    }

    private static int spread(int hash) {
        // Spreads (XORs) higher bits of hash to lower bits.
        // This reduces hash collisions when the table size is small, as the index calculation
        // (hash & (n - 1)) only uses the lower bits. By mixing high bits into low bits,
        // we ensure that differences in high bits also affect the index.
        hash ^= (hash >>> 16);
        return hash;
    }

    private static int ceilingNextPowerOfTwo(int value) {
        if (value >= (1 << 30)) {
            return 1 << 30;
        }
        int highestOneBit = Integer.highestOneBit(value - 1);
        return Math.max(1, highestOneBit << 1);
    }

    private static final class Segment<V> {
        private final Long2ObjectOpenHashMap<V> map;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        private Segment(int expectedSize) {
            if (expectedSize > 0) {
                this.map = new Long2ObjectOpenHashMap<>(expectedSize);
            } else {
                this.map = new Long2ObjectOpenHashMap<>();
            }
        }
    }
}
