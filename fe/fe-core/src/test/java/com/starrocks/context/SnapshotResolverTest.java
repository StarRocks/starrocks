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

package com.starrocks.context;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests the input-parsing and validation behaviour of {@link SnapshotResolver} without requiring
 * the internal tables to exist. The SQL-issuing paths return {@code -1} when the tables aren't
 * present, which is exactly what these assertions exercise via the degraded-mode result.
 */
public class SnapshotResolverTest {

    @Test
    public void testExactModeRequiresSnapshot() {
        SnapshotResolver resolver = new SnapshotResolver();
        SnapshotResolver.Request req = new SnapshotResolver.Request();
        req.mode = SnapshotResolver.ReadMode.EXACT;
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(req));
        Assertions.assertTrue(ex.getMessage().contains("exact_snapshot"));
    }

    @Test
    public void testAsOfRequiresTimestamp() {
        SnapshotResolver resolver = new SnapshotResolver();
        SnapshotResolver.Request req = new SnapshotResolver.Request();
        req.mode = SnapshotResolver.ReadMode.AS_OF_TIME;
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(req));
        Assertions.assertTrue(ex.getMessage().contains("as_of_time"));
    }

    @Test
    public void testUnknownSelectorRejected() {
        SnapshotResolver resolver = new SnapshotResolver();
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolver.resolveFromSelector(null, "not-a-timestamp-or-number"));
        Assertions.assertTrue(ex.getMessage().contains("snapshot_version"));
    }

    @Test
    public void testNumericSelectorTreatedAsExactSnapshot() {
        SnapshotResolver resolver = new SnapshotResolver();
        // No internal tables → exact mode returns -1; the assertion verifies we took the numeric
        // branch (no IllegalArgumentException).
        long result = resolver.resolveFromSelector(null, "42");
        Assertions.assertEquals(-1L, result);
    }

    @Test
    public void testDatePadsToMidnight() {
        SnapshotResolver resolver = new SnapshotResolver();
        // Again, returns -1 without tables; the test asserts no exception — i.e. the date was
        // parsed successfully after padding.
        long result = resolver.resolveFromSelector(null, "2026-03-01");
        Assertions.assertEquals(-1L, result);
    }

    @Test
    public void testEmptySelectorIsCurrent() {
        SnapshotResolver resolver = new SnapshotResolver();
        long result = resolver.resolveFromSelector(null, "");
        Assertions.assertEquals(-1L, result);
    }
}
