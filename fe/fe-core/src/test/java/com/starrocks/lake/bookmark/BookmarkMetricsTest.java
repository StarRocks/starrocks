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
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkMetricsTest extends BookmarkTestBase {

    @Test
    public void testApplyHooksBumpEverything() {
        BookmarkMetrics m = new BookmarkMetrics();
        MetricRegistry registry = new MetricRegistry();
        m.register(registry);

        // 1. onBookmarkCreated bumps cardinality (bookmark / reference / logical
        //    / physical partition) AND cumulative *_total counters.
        m.onBookmarkCreated(2, 3L, 4L);
        assertEquals(1L, m.bookmarkCount.longValue());
        assertEquals(2L, m.bookmarkReferenceCount.longValue());
        assertEquals(3L, m.bookmarkLogicalPartitionCount.longValue());
        assertEquals(4L, m.bookmarkPhysicalPartitionCount.longValue());
        assertEquals(1L, m.bookmarkCreatedTotal.longValue());
        assertEquals(2L, m.bookmarkReferenceAddedTotal.longValue());

        // 2. onReferenceAdded bumps the reference cardinality and the
        //    reference-added cumulative.
        m.onReferenceAdded();
        assertEquals(3L, m.bookmarkReferenceCount.longValue());
        assertEquals(3L, m.bookmarkReferenceAddedTotal.longValue());

        // 3. onReferenceReleased decrements the cardinality, bumps released_total
        //    and the reference-age histogram. TTL expiry is a separate extra bump
        //    (ttl_expired_total), not a substitute for this hook.
        m.onReferenceReleased(45L);
        assertEquals(2L, m.bookmarkReferenceCount.longValue());
        assertEquals(1L, m.bookmarkReferenceReleasedTotal.longValue());
        assertEquals(1L, registry.histogram("bookmark_reference_completed_age_ms").getCount());
        assertEquals(0L, m.bookmarkReferenceTtlExpiredTotal.longValue());

        m.onReferenceTtlExpired();
        assertEquals(1L, m.bookmarkReferenceTtlExpiredTotal.longValue());
        assertEquals(1L, m.bookmarkReferenceReleasedTotal.longValue());

        // 4. onBookmarkRemoved decrements bookmark + logical/physical partition
        //    cardinality, bumps cumulative, records the bookmark-age histogram.
        m.onBookmarkRemoved(500L, 3L, 4L);
        assertEquals(0L, m.bookmarkCount.longValue());
        assertEquals(0L, m.bookmarkLogicalPartitionCount.longValue());
        assertEquals(0L, m.bookmarkPhysicalPartitionCount.longValue());
        assertEquals(1L, m.bookmarkRemovedTotal.longValue());
        assertEquals(1L, registry.histogram("bookmark_completed_age_ms").getCount());

        // 5. A second register() on the same registry must not throw —
        //    histograms are get-or-create, so a re-init is idempotent.
        m.register(registry);
    }

    @Test
    public void testAddBookmarkCounts() {
        BookmarkMetrics m = new BookmarkMetrics();

        // Add a batch of 2 bookmarks, 5 references, 7 logical partitions,
        // 9 physical partitions.
        m.addBookmarkCounts(2L, 5L, 7L, 9L);
        assertEquals(2L, m.bookmarkCount.longValue());
        assertEquals(5L, m.bookmarkReferenceCount.longValue());
        assertEquals(7L, m.bookmarkLogicalPartitionCount.longValue());
        assertEquals(9L, m.bookmarkPhysicalPartitionCount.longValue());
        // Cumulative *_total counters reset per process per Prometheus
        // convention — they are not seeded by the image.
        assertEquals(0L, m.bookmarkCreatedTotal.longValue());
        assertEquals(0L, m.bookmarkReferenceAddedTotal.longValue());

        // A subsequent live hook composes additively with the seed.
        m.onBookmarkCreated(1, 2L, 3L);
        assertEquals(3L, m.bookmarkCount.longValue());
        assertEquals(6L, m.bookmarkReferenceCount.longValue());
        assertEquals(9L, m.bookmarkLogicalPartitionCount.longValue());
        assertEquals(12L, m.bookmarkPhysicalPartitionCount.longValue());
    }

    @Test
    public void testSetMaxActiveAges() {
        BookmarkMetrics m = new BookmarkMetrics();
        // Initial values default to 0 — gauges report 0 until the first refresh.
        assertEquals(0L, m.bookmarkMaxActiveAgeMs.get());
        assertEquals(0L, m.bookmarkReferenceMaxActiveAgeMs.get());

        m.setMaxActiveAges(123L, 45L);
        assertEquals(123L, m.bookmarkMaxActiveAgeMs.get());
        assertEquals(45L, m.bookmarkReferenceMaxActiveAgeMs.get());

        // A subsequent refresh overwrites — values reflect the most recent walk.
        m.setMaxActiveAges(0L, 0L);
        assertEquals(0L, m.bookmarkMaxActiveAgeMs.get());
        assertEquals(0L, m.bookmarkReferenceMaxActiveAgeMs.get());
    }

    @Test
    public void testRegisterPublishesAllMetrics() {
        BookmarkMetrics m = new BookmarkMetrics();
        MetricRegistry registry = new MetricRegistry();
        m.register(registry);

        // Drive internal state: 1 bookmark with 3 logical / 4 physical
        // partitions and 1 ref, plus the cached max ages. The 6 published
        // metrics must all reflect those values.
        m.onBookmarkCreated(1, 3L, 4L);
        m.setMaxActiveAges(123L, 45L);

        assertMetricPublished("bookmark_count", 1L);
        assertMetricPublished("bookmark_reference_count", 1L);
        assertMetricPublished("bookmark_logical_partition_count", 3L);
        assertMetricPublished("bookmark_physical_partition_count", 4L);
        assertMetricPublished("bookmark_max_active_age_ms", 123L);
        assertMetricPublished("bookmark_reference_max_active_age_ms", 45L);
    }

    // MetricRepo is process-global: every BookmarkMetrics instance any test registers adds
    // another same-named metric bound to its own source, so a positional pick can land on a
    // different instance. Match by value to target the instance driven by this test.
    @SuppressWarnings("unchecked")
    private static void assertMetricPublished(String name, long expected) {
        List<Metric> ms = MetricRepo.getMetricsByName(name);
        assertFalse(ms.isEmpty(), "metric not registered: " + name);
        assertTrue(ms.stream().anyMatch(g -> ((Metric<Long>) g).getValue().longValue() == expected),
                "no published '" + name + "' metric reports " + expected);
    }
}
