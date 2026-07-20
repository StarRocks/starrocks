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

package com.starrocks.connector.partitiontraits;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// Tests the delete-aware live-row total used to gate bounded-cost extrapolation (PR #76549 review, comment 1):
// record_count from PARTITIONS is pre-delete, so it must not be trusted as a live-row total when deletes are
// significant, otherwise a fully-scanned MOR partition looks "truncated".
public class IcebergPartitionRowCountTest {

    @Test
    public void testNoDeletesUsesRecordCount() {
        Assertions.assertEquals(1000, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 0, 0));
        // Missing (-1) delete counts are treated as zero.
        Assertions.assertEquals(1000, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, -1, -1));
    }

    @Test
    public void testPositionDeletesSubtractedExactly() {
        // 5% position deletes (<= 10% gate): live total = 1000 - 50.
        Assertions.assertEquals(950, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 50, 0));
    }

    @Test
    public void testLowEqualityDeletesSubtracted() {
        // 8% combined deletes (<= 10% gate): live total = 1000 - 30 - 50.
        Assertions.assertEquals(920, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 30, 50));
    }

    @Test
    public void testHighDeleteRatioSkips() {
        // 20% deletes (> 10% gate): record_count is unreliable as a live total -> skip (-1 -> raw sample).
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 100, 100));
        // Mostly-deleted partition: definitely skip (this is the catastrophic case the gate protects against).
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 950, 0));
    }

    @Test
    public void testUnknownRecordCountSkips() {
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(-1, 0, 0));
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(0, 0, 0));
    }

    @Test
    public void testAtGateBoundaryExtrapolates() {
        // Exactly 10% deletes is within the gate (strictly-greater-than check).
        Assertions.assertEquals(900, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 100, 0));
    }
}
