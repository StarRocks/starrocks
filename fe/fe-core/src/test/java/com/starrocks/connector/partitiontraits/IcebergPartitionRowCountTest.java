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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorPartitionTraits;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

// Tests the delete-aware live-row total used by bounded-cost extrapolation. record_count from PARTITIONS is
// pre-delete; subtracting position deletes gives a lower bound of the live rows (a dangling delete removes no
// row), which extrapolation only scales a truncated sample up to.
public class IcebergPartitionRowCountTest {

    @Test
    public void testNoDeletesUsesRecordCount() {
        Assertions.assertEquals(1000, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 0, 0));
        // Missing (-1) delete counts are treated as zero.
        Assertions.assertEquals(1000, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, -1, -1));
    }

    @Test
    public void testPositionDeletesSubtractedExactly() {
        // live total = 1000 - 50.
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
    public void testHighPositionDeleteRatioStillExtrapolates() {
        // A high position delete ratio still yields a lower bound of the live rows: dangling deletes only make
        // the real total larger, and extrapolation never scales above the bound.
        Assertions.assertEquals(800, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 200, 0));
        Assertions.assertEquals(50, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 950, 0));
        // Over-counted deletes (more delete records than rows) clamp to 1.
        Assertions.assertEquals(1, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 2000, 0));
    }

    @Test
    public void testUnknownRecordCountSkips() {
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(-1, 0, 0));
        Assertions.assertEquals(-1, IcebergPartitionTraits.liveRowCountForExtrapolation(0, 0, 0));
    }

    @Test
    public void testTenPercentDeletesExtrapolates() {
        Assertions.assertEquals(900, IcebergPartitionTraits.liveRowCountForExtrapolation(1000, 100, 0));
    }

    @Test
    public void testTableLevelEqualityDeletesSkipAllPartitions() {
        // An equality delete under an unpartitioned spec applies to every partition while each partition's own
        // counts show none, so the snapshot summary decides for the whole table, before any partition is read.
        org.apache.iceberg.Table nativeTable = Mockito.mock(org.apache.iceberg.Table.class);
        Snapshot withEqDeletes = Mockito.mock(Snapshot.class);
        Mockito.when(withEqDeletes.summary()).thenReturn(ImmutableMap.of(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "3"));
        Mockito.when(nativeTable.snapshot(42L)).thenReturn(withEqDeletes);
        IcebergTable icebergTable = new IcebergTable(0, "name", "iceberg_catalog", "resource_name", "icebergDb",
                "icebergTable", "", Lists.newArrayList(), nativeTable, Maps.newHashMap());

        ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(icebergTable, TvrTableSnapshot.of(42L));
        Assertions.assertTrue(traits.getPartitionRowCounts(Lists.newArrayList("p1")).isEmpty());
    }

    @Test
    public void testOnlyReportedEqualityDeletesBlockTheTable() {
        Assertions.assertTrue(IcebergPartitionTraits.snapshotHasEqualityDeletes(snapshotWithSummary(
                ImmutableMap.of(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "3"))));
        Assertions.assertFalse(IcebergPartitionTraits.snapshotHasEqualityDeletes(snapshotWithSummary(
                ImmutableMap.of(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0"))));
        // Some writers omit the totals: fall back to the per-partition counts rather than give up on the table.
        Assertions.assertFalse(IcebergPartitionTraits.snapshotHasEqualityDeletes(snapshotWithSummary(
                ImmutableMap.of(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0"))));
        Assertions.assertFalse(IcebergPartitionTraits.snapshotHasEqualityDeletes(null));
    }

    private static Snapshot snapshotWithSummary(Map<String, String> summary) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.summary()).thenReturn(summary);
        return snapshot;
    }
}
