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
    public void testAnyEqualityDeletesSkip() {
        // equality_delete_record_count counts predicates, not rows removed, so a low count is NOT safe:
        // one predicate can match many rows. Presence of any equality deletes -> skip (-1 -> raw sample),
        // even at a tiny record ratio.
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 0, 1));
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(1_000_000, 0, 10));
        // Even with acceptable position deletes, any equality delete forces a skip.
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 50, 5));
    }

    @Test
    public void testHighPositionDeleteRatioSkips() {
        // 20% position deletes (> 10% gate): record_count is unreliable as a live total -> skip.
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 200, 0));
        // Mostly-deleted partition: definitely skip (the catastrophic case the gate protects against).
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
