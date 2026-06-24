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

        // 3. onReferenceReleased decrements the cardinality, bumps the
        //    cumulative + records the reference-age histogram.
        m.onReferenceReleased(45L);
        assertEquals(2L, m.bookmarkReferenceCount.longValue());
        assertEquals(1L, m.bookmarkReferenceReleasedTotal.longValue());
        assertEquals(1L, registry.histogram("bookmark_reference_completed_age_ms").getCount());

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

        assertEquals(1L, latestMetricValue("bookmark_count"));
        assertEquals(1L, latestMetricValue("bookmark_reference_count"));
        assertEquals(3L, latestMetricValue("bookmark_logical_partition_count"));
        assertEquals(4L, latestMetricValue("bookmark_physical_partition_count"));
        assertEquals(123L, latestMetricValue("bookmark_max_active_age_ms"));
        assertEquals(45L, latestMetricValue("bookmark_reference_max_active_age_ms"));
    }

    @SuppressWarnings("unchecked")
    private static long latestMetricValue(String name) {
        List<Metric> ms = MetricRepo.getMetricsByName(name);
        assertFalse(ms.isEmpty(), "metric not registered: " + name);
        Metric<Long> g = (Metric<Long>) ms.get(ms.size() - 1);
        return g.getValue();
    }
}
