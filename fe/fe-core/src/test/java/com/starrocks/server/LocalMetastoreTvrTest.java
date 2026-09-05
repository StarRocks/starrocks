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

package com.starrocks.server;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.exception.TvrAncestryBrokenException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for LocalMetastore's two TVR methods: getCurrentTvrSnapshot and listTableDeltaTraits.
 * These tests set partition epochs directly, so they check the watermark math (max, no
 * overflow) on its own. A separate test checks that a real commit bumps the epoch.
 */
public class LocalMetastoreTvrTest {

    private LocalMetastore store;

    @BeforeEach
    public void setUp() {
        store = mock(LocalMetastore.class, CALLS_REAL_METHODS);
    }

    private OlapTable olapTableWithEpochs(long... epochs) {
        OlapTable table = mock(OlapTable.class);
        List<PhysicalPartition> partitions = new ArrayList<>();
        for (long e : epochs) {
            PhysicalPartition p = mock(PhysicalPartition.class);
            when(p.getVersionEpoch()).thenReturn(e);
            partitions.add(p);
        }
        when(table.getPhysicalPartitions()).thenReturn(partitions);
        return table;
    }

    // ---- getCurrentTvrSnapshot: watermark = MAX(versionEpoch) ----

    @Test
    public void testSnapshot_isMaxEpochNotSum() {
        // Watermark should be the highest epoch (5), not the sum (10).
        OlapTable table = olapTableWithEpochs(3L, 5L, 2L);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertFalse(snap.isEmpty());
        assertEquals(5L, snap.getSnapshotId());
    }

    @Test
    public void testSnapshot_advancesOnLoadIntoAnyPartition() {
        // Every commit gets a new, bigger epoch. So the watermark should go up no matter
        // which partition was loaded — even one that did not hold the old max.
        long before = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(3L, 5L, 2L)).getSnapshotId();
        // load into the partition that held the max (5 -> 20)
        long afterLoadMax = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(3L, 20L, 2L)).getSnapshotId();
        // load into a different partition (3 -> 30); the watermark still moves
        long afterLoadNonMax = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(30L, 5L, 2L)).getSnapshotId();
        assertEquals(5L, before);
        assertTrue(afterLoadMax > before, "load into max partition must advance the watermark");
        assertTrue(afterLoadNonMax > before, "load into a non-max partition must advance the watermark");
    }

    @Test
    public void testSnapshot_manyLargeEpochs_doesNotOverflow() {
        // Real epochs are huge numbers (~4.3e17). Adding 30 of them would overflow a long
        // and go negative. Taking the max instead keeps one valid, positive number.
        final long base = 433_000_000_000_000_000L; // ~4.33e17, a realistic epoch size
        long[] epochs = new long[30];
        long sum = 0;
        long expectedMax = 0;
        for (int i = 0; i < epochs.length; i++) {
            epochs[i] = base + i;
            sum += epochs[i];
            expectedMax = Math.max(expectedMax, epochs[i]);
        }
        assertTrue(sum < 0, "sanity check: adding these epochs overflows to negative");

        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(epochs));
        assertFalse(snap.isEmpty());
        assertEquals(expectedMax, snap.getSnapshotId());
        assertTrue(snap.getSnapshotId() > 0, "watermark must stay positive (no overflow)");
    }

    @Test
    public void testSnapshot_noPartitions_returnsEmpty() {
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", olapTableWithEpochs());
        assertTrue(snap.isEmpty());
    }

    @Test
    public void testSnapshot_nonOlapTable_returnsEmpty() {
        Table table = mock(Table.class);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertTrue(snap.isEmpty());
    }

    // ---- listTableDeltaTraits ----

    @Test
    public void testDeltaTraits_native_isRetractable() {
        // Every native delta is marked RETRACTABLE, since any key type can have deletes.
        OlapTable table = olapTableWithEpochs(5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testDeltaTraits_emptyFrom_deltaStartsFromMin() {
        OlapTable table = olapTableWithEpochs(5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertTrue(traits.get(0).getTvrDelta().start().isEmpty());
        assertEquals(Optional.of(5L), traits.get(0).getTvrDelta().end());
    }

    @Test
    public void testDeltaTraits_fromEqualsTo_returnsEmpty() {
        OlapTable table = olapTableWithEpochs(5L);
        TvrTableSnapshot snap = TvrTableSnapshot.of(5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits("db", table, snap, snap);
        assertTrue(traits.isEmpty());
    }

    @Test
    public void testDeltaTraits_nonOlapTable_returnsEmpty() {
        Table table = mock(Table.class);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertTrue(traits.isEmpty());
    }

    @Test
    public void testDeltaTraits_fromGreaterThanTo_throwsTypedAncestryError() {
        // Models dropping the partition with the max epoch: the MV's last-seen watermark (7) is
        // now bigger than the table's current one (3). Must throw the typed ancestry error, so
        // MVIVMRefreshProcessor shows the "drop and recreate the MV" message.
        OlapTable table = olapTableWithEpochs(3L);
        TvrTableSnapshot from = TvrTableSnapshot.of(7L);
        TvrTableSnapshot to = TvrTableSnapshot.of(3L);
        TvrAncestryBrokenException ex = assertThrows(TvrAncestryBrokenException.class,
                () -> store.listTableDeltaTraits("db", table, from, to));
        assertTrue(ex.getMessage().contains("is not a parent ancestor"));
    }

    @Test
    public void testDeltaTraits_emptyToSnapshot_throwsTypedAncestryError() {
        // An empty TARGET means every partition was dropped. That is a broken history too, so
        // it must throw the same typed ancestry error as testDeltaTraits_fromGreaterThanTo above.
        OlapTable table = olapTableWithEpochs(5L);
        TvrTableSnapshot from = TvrTableSnapshot.of(3L);
        assertThrows(TvrAncestryBrokenException.class,
                () -> store.listTableDeltaTraits("db", table, from, TvrTableSnapshot.empty()));
    }

    @Test
    public void testDeltaTraits_emptyToWithEmptyFrom_shortCircuitsEmpty() {
        // from and to are both empty (no change), so this returns an empty list right away.
        OlapTable table = olapTableWithEpochs(5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.empty());
        assertTrue(traits.isEmpty());
    }

    @Test
    public void testDeltaTraits_typedAncestryExceptionIsConnectorException() {
        // The processor catches StarRocksConnectorException, so this type must extend it.
        assertTrue(new TvrAncestryBrokenException("history is broken")
                instanceof StarRocksConnectorException);
    }
}
