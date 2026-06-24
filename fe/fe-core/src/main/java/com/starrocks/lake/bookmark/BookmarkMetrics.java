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

package com.starrocks.lake.bookmark;

import com.codahale.metrics.MetricRegistry;
import com.starrocks.metric.LeaderAwareCounterMetric;
import com.starrocks.metric.LeaderAwareGaugeMetricLong;
import com.starrocks.metric.LeaderAwareHistogramMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Bookmark module's Prometheus counters, gauges, and histograms. */
final class BookmarkMetrics {

    // Cumulative count of bookmarks created since process start.
    private static final String NAME_BOOKMARK_CREATED_TOTAL = "bookmark_created_total";
    final LongAdder bookmarkCreatedTotal = new LongAdder();

    // Cumulative count of bookmarks removed since process start.
    private static final String NAME_BOOKMARK_REMOVED_TOTAL = "bookmark_removed_total";
    final LongAdder bookmarkRemovedTotal = new LongAdder();

    // Cumulative count of bookmark references added since process start.
    private static final String NAME_BOOKMARK_REFERENCE_ADDED_TOTAL = "bookmark_reference_added_total";
    final LongAdder bookmarkReferenceAddedTotal = new LongAdder();

    // Cumulative count of bookmark references released since process start.
    private static final String NAME_BOOKMARK_REFERENCE_RELEASED_TOTAL = "bookmark_reference_released_total";
    final LongAdder bookmarkReferenceReleasedTotal = new LongAdder();

    // Current count of active bookmarks.
    private static final String NAME_BOOKMARK_COUNT = "bookmark_count";
    final LongAdder bookmarkCount = new LongAdder();

    // Current count of active bookmark references.
    private static final String NAME_BOOKMARK_REFERENCE_COUNT = "bookmark_reference_count";
    final LongAdder bookmarkReferenceCount = new LongAdder();

    // Sum of per-bookmark logical partition counts across all active bookmarks
    // (a logical partition captured by N bookmarks contributes N to the sum).
    private static final String NAME_BOOKMARK_LOGICAL_PARTITION_COUNT = "bookmark_logical_partition_count";
    final LongAdder bookmarkLogicalPartitionCount = new LongAdder();

    // Sum of per-bookmark physical partition counts across all active bookmarks
    // (a physical partition captured by N bookmarks contributes N to the sum).
    private static final String NAME_BOOKMARK_PHYSICAL_PARTITION_COUNT = "bookmark_physical_partition_count";
    final LongAdder bookmarkPhysicalPartitionCount = new LongAdder();

    // Age of the oldest currently-active bookmark. Cached so scrapes don't
    // walk the trackers; BookmarkManager refreshes it on its cleanup cycle.
    private static final String NAME_BOOKMARK_MAX_ACTIVE_AGE_MS = "bookmark_max_active_age_ms";
    final AtomicLong bookmarkMaxActiveAgeMs = new AtomicLong(0L);

    // Age of the oldest currently-active bookmark reference. Refreshed on the
    // same cycle as bookmarkMaxActiveAgeMs.
    private static final String NAME_BOOKMARK_REFERENCE_MAX_ACTIVE_AGE_MS = "bookmark_reference_max_active_age_ms";
    final AtomicLong bookmarkReferenceMaxActiveAgeMs = new AtomicLong(0L);

    // Distribution of bookmark lifetimes (creation to last-reference release).
    // Histograms own their own state and the LeaderAware variant needs a
    // ready GlobalStateMgr, so they're resolved in register(); the
    // Optional.empty() default makes pre-registration apply() calls a silent
    // no-op instead of an NPE.
    private static final String NAME_BOOKMARK_COMPLETED_AGE_MS = "bookmark_completed_age_ms";
    private Optional<LeaderAwareHistogramMetric> bookmarkCompletedAgeMs = Optional.empty();

    // Distribution of bookmark-reference lifetimes (acquire to release).
    private static final String NAME_BOOKMARK_REFERENCE_COMPLETED_AGE_MS = "bookmark_reference_completed_age_ms";
    private Optional<LeaderAwareHistogramMetric> bookmarkReferenceCompletedAgeMs = Optional.empty();

    private boolean registered;

    // Hooks below run on both replay and live apply — neither path skips them.

    void onBookmarkCreated(int initialReferenceCount, long logicalPartitionCount, long physicalPartitionCount) {
        bookmarkCreatedTotal.increment();
        bookmarkReferenceAddedTotal.add(initialReferenceCount);
        bookmarkCount.increment();
        bookmarkReferenceCount.add(initialReferenceCount);
        bookmarkLogicalPartitionCount.add(logicalPartitionCount);
        bookmarkPhysicalPartitionCount.add(physicalPartitionCount);
    }

    void onBookmarkRemoved(long ageMs, long logicalPartitionCount, long physicalPartitionCount) {
        bookmarkRemovedTotal.increment();
        bookmarkCount.decrement();
        bookmarkLogicalPartitionCount.add(-logicalPartitionCount);
        bookmarkPhysicalPartitionCount.add(-physicalPartitionCount);
        bookmarkCompletedAgeMs.ifPresent(h -> h.update(ageMs));
    }

    void onReferenceAdded() {
        bookmarkReferenceAddedTotal.increment();
        bookmarkReferenceCount.increment();
    }

    void onReferenceReleased(long ageMs) {
        bookmarkReferenceReleasedTotal.increment();
        bookmarkReferenceCount.decrement();
        bookmarkReferenceCompletedAgeMs.ifPresent(h -> h.update(ageMs));
    }

    /**
     * Adds the given counts to the running bookmark, reference, and logical/physical partition
     * gauges. The cumulative *_total counters are left untouched — they reset per process per the
     * Prometheus convention.
     */
    void addBookmarkCounts(long bookmarkCount, long referenceCount, long logicalPartitionCount, long physicalPartitionCount) {
        this.bookmarkCount.add(bookmarkCount);
        this.bookmarkReferenceCount.add(referenceCount);
        this.bookmarkLogicalPartitionCount.add(logicalPartitionCount);
        this.bookmarkPhysicalPartitionCount.add(physicalPartitionCount);
    }

    /** Overwrite the cached max-age values; scrapes read them directly. */
    void setMaxActiveAges(long maxBookmarkAgeMs, long maxReferenceAgeMs) {
        bookmarkMaxActiveAgeMs.set(maxBookmarkAgeMs);
        bookmarkReferenceMaxActiveAgeMs.set(maxReferenceAgeMs);
    }

    /**
     * Publishes every bookmark metric. Counters and gauges go to MetricRepo as
     * leader-aware variants so follower FEs report zero (and carry an
     * is_leader="false" label) instead of double-counting against the leader's
     * values; histograms go to the passed-in registry as the leader-aware
     * variant so follower snapshots collapse to NOOP. Idempotent — guarded so
     * a second call from a re-init path doesn't append duplicates.
     */
    void register(MetricRegistry registry) {
        if (registered) {
            return;
        }
        registered = true;

        registerCounter(NAME_BOOKMARK_CREATED_TOTAL, MetricUnit.OPERATIONS,
                "total bookmarks created", bookmarkCreatedTotal);
        registerCounter(NAME_BOOKMARK_REMOVED_TOTAL, MetricUnit.OPERATIONS,
                "total bookmarks removed (last reference released)", bookmarkRemovedTotal);
        registerCounter(NAME_BOOKMARK_REFERENCE_ADDED_TOTAL, MetricUnit.OPERATIONS,
                "total bookmark references added", bookmarkReferenceAddedTotal);
        registerCounter(NAME_BOOKMARK_REFERENCE_RELEASED_TOTAL, MetricUnit.OPERATIONS,
                "total bookmark references released", bookmarkReferenceReleasedTotal);

        registerGauge(NAME_BOOKMARK_COUNT, MetricUnit.NOUNIT,
                "current count of active bookmarks", bookmarkCount);
        registerGauge(NAME_BOOKMARK_REFERENCE_COUNT, MetricUnit.NOUNIT,
                "current count of active bookmark references", bookmarkReferenceCount);
        registerGauge(NAME_BOOKMARK_LOGICAL_PARTITION_COUNT, MetricUnit.NOUNIT,
                "sum of per-bookmark logical partition counts across all active bookmarks "
                        + "(a partition captured by N bookmarks is counted N times)",
                bookmarkLogicalPartitionCount);
        registerGauge(NAME_BOOKMARK_PHYSICAL_PARTITION_COUNT, MetricUnit.NOUNIT,
                "sum of per-bookmark physical partition counts across all active bookmarks "
                        + "(a partition captured by N bookmarks is counted N times)",
                bookmarkPhysicalPartitionCount);

        registerAgeGauge(NAME_BOOKMARK_MAX_ACTIVE_AGE_MS,
                "age of the oldest currently-active bookmark", bookmarkMaxActiveAgeMs);
        registerAgeGauge(NAME_BOOKMARK_REFERENCE_MAX_ACTIVE_AGE_MS,
                "age of the oldest currently-active bookmark reference", bookmarkReferenceMaxActiveAgeMs);

        bookmarkCompletedAgeMs = Optional.of(registerHistogram(registry, NAME_BOOKMARK_COMPLETED_AGE_MS));
        bookmarkReferenceCompletedAgeMs =
                Optional.of(registerHistogram(registry, NAME_BOOKMARK_REFERENCE_COMPLETED_AGE_MS));
    }

    private static void registerCounter(String name, MetricUnit unit, String desc, LongAdder source) {
        MetricRepo.addMetric(new LeaderAwareCounterMetric<Long>(name, unit, desc) {
            @Override
            public void increase(Long delta) {
                source.add(delta);
            }

            @Override
            public Long getValueLeader() {
                return source.longValue();
            }

            @Override
            public Long getValueNonLeader() {
                return 0L;
            }
        });
    }

    private static void registerGauge(String name, MetricUnit unit, String desc, LongAdder source) {
        MetricRepo.addMetric(new LeaderAwareGaugeMetricLong(name, unit, desc) {
            @Override
            public Long getValueLeader() {
                return source.longValue();
            }
        });
    }

    private static void registerAgeGauge(String name, String desc, AtomicLong source) {
        MetricRepo.addMetric(new LeaderAwareGaugeMetricLong(name, MetricUnit.MILLISECONDS, desc) {
            @Override
            public Long getValueLeader() {
                return source.get();
            }
        });
    }

    private static LeaderAwareHistogramMetric registerHistogram(MetricRegistry registry, String name) {
        LeaderAwareHistogramMetric h = new LeaderAwareHistogramMetric(name);
        registry.register(name, h);
        return h;
    }
}
