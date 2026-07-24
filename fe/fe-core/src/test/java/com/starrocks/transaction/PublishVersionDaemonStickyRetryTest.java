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

package com.starrocks.transaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure per-partition sticky-retry decision helpers shared by the single-transaction and batch
 * lake publish paths (SR-38350). versionTime sentinel: 0 = never run, &gt; 0 = last publish succeeded at that
 * time, &lt; 0 = last publish failed at |versionTime|.
 */
public class PublishVersionDaemonStickyRetryTest {

    @Test
    public void testPartitionPublished() {
        assertTrue(PublishVersionDaemon.partitionPublished(100));   // succeeded
        assertFalse(PublishVersionDaemon.partitionPublished(0));    // never run
        assertFalse(PublishVersionDaemon.partitionPublished(-100)); // failed
    }

    @Test
    public void testBackoffNeverRan() {
        // versionTime == 0 (fresh) is never in backoff: a partition that never ran publishes immediately.
        assertFalse(PublishVersionDaemon.partitionInBackoff(0, 5_000, 1_000));
    }

    @Test
    public void testBackoffAlreadyPublished() {
        // versionTime > 0 (already published) is never in backoff.
        assertFalse(PublishVersionDaemon.partitionInBackoff(100, 5_000, 1_000));
    }

    @Test
    public void testBackoffWithinWindow() {
        // failed at t=5000, interval 1000, now=5500 -> within [5000, 6000) -> backed off
        assertTrue(PublishVersionDaemon.partitionInBackoff(-5_000, 5_500, 1_000));
    }

    @Test
    public void testBackoffWindowElapsed() {
        // failed at t=5000, interval 1000, now=6000 -> window elapsed -> retry allowed
        assertFalse(PublishVersionDaemon.partitionInBackoff(-5_000, 6_000, 1_000));
    }

    @Test
    public void testBackoffZeroInterval() {
        // interval 0 -> never backed off (retry on the next cycle)
        assertFalse(PublishVersionDaemon.partitionInBackoff(-5_000, 5_000, 0));
    }
}
