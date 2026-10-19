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

import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionCommitInfoTest {
    private static final long PID = 12345L;

    @Test
    public void testShouldLogPublishErrorThrottlesRepeatedFailures() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, 2, 0);
        long t0 = 1_000_000L;

        // The first failure is always worth a line.
        Assertions.assertTrue(pci.shouldLogPublishError(t0, 10_000L));
        // A partition that keeps failing retries about once a second; those must not each log.
        Assertions.assertFalse(pci.shouldLogPublishError(t0 + 1_000L, 10_000L));
        Assertions.assertFalse(pci.shouldLogPublishError(t0 + 5_000L, 10_000L));
        Assertions.assertFalse(pci.shouldLogPublishError(t0 + 9_999L, 10_000L));
        // Once the interval has elapsed the partition is still stuck, so say so again.
        Assertions.assertTrue(pci.shouldLogPublishError(t0 + 10_000L, 10_000L));
        Assertions.assertFalse(pci.shouldLogPublishError(t0 + 10_001L, 10_000L));
    }

    @Test
    public void testShouldLogPublishErrorIsNotPersisted() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, 2, 0);
        Assertions.assertTrue(pci.shouldLogPublishError(1_000_000L, 10_000L));

        // The throttle only paces logging inside one process, so it must not travel through the
        // image; a replayed copy starts fresh rather than staying silent.
        PartitionCommitInfo copied = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pci), PartitionCommitInfo.class);
        Assertions.assertTrue(copied.shouldLogPublishError(1_000_001L, 10_000L));
    }

    @Test
    public void testPublishFailureIsSeparateFromVersionTime() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, 2, 0);
        Assertions.assertEquals(0, pci.getLastPublishFailureTime());

        // A failed attempt must not negate versionTime: that field is the timestamp handed to
        // Partition#updateVisibleVersion, and it is serialized into the image.
        pci.markPublishFailed(1_000_000L);
        Assertions.assertEquals(1_000_000L, pci.getLastPublishFailureTime());
        Assertions.assertEquals(0, pci.getVersionTime());

        pci.markPublishSucceeded(1_000_500L);
        Assertions.assertEquals(1_000_500L, pci.getVersionTime());
        Assertions.assertEquals(0, pci.getLastPublishFailureTime());

        // The retry state is per process, so it must not survive the image either.
        pci.markPublishFailed(1_001_000L);
        PartitionCommitInfo copied = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pci), PartitionCommitInfo.class);
        Assertions.assertEquals(0, copied.getLastPublishFailureTime());
        Assertions.assertEquals(1_000_500L, copied.getVersionTime());
    }
}
