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
 * Unit tests for the native-table TVR bridge in {@link LocalMetastore}:
 * {@code getCurrentTvrSnapshot} and {@code listTableDeltaTraits}.
 *
 * <p>These tests control the per-partition version epochs directly, so they verify the watermark
 * <em>formula</em> (MAX of per-partition epochs) in isolation: it advances when any partition's epoch
 * grows, and it does not overflow. The complementary invariant — that a commit actually stamps a
 * fresh, larger epoch on the written partition (so a normal load moves the watermark) — is enforced
 * in {@code DatabaseTransactionMgr} and covered by its own test; see the epoch-advance PR.
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
        // Watermark must be the MAX epoch (5), never the sum (10). Summing was the overflow bug.
        OlapTable table = olapTableWithEpochs(3L, 5L, 2L);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertFalse(snap.isEmpty());
        assertEquals(5L, snap.getSnapshotId());
    }

    @Test
    public void testSnapshot_advancesOnLoadIntoAnyPartition() {
        // A commit stamps a globally-largest epoch on the written partition; the MAX therefore
        // advances no matter which partition was loaded — including a non-max one.
        long before = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(3L, 5L, 2L)).getSnapshotId();
        // load into the partition that held the max (5 -> 20)
        long afterLoadMax = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(3L, 20L, 2L)).getSnapshotId();
        // load into a non-max partition (3 -> 30); MAX still advances
        long afterLoadNonMax = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(30L, 5L, 2L)).getSnapshotId();
        assertEquals(5L, before);
        assertTrue(afterLoadMax > before, "load into max partition must advance the watermark");
        assertTrue(afterLoadNonMax > before, "load into a non-max partition must advance the watermark");
    }

    @Test
    public void testSnapshot_manyLargeEpochs_doesNotOverflow() {
        // Real GTID epochs are ~4.3e17. With 30 partitions the SUM overflows signed long (goes
        // negative); MAX stays a single valid, positive epoch. This is the C2 regression guard.
        final long base = 433_000_000_000_000_000L; // ~4.33e17, 2026-era GTID magnitude
        long[] epochs = new long[30];
        long sum = 0;
        long expectedMax = 0;
        for (int i = 0; i < epochs.length; i++) {
            epochs[i] = base + i;
            sum += epochs[i];
            expectedMax = Math.max(expectedMax, epochs[i]);
        }
        assertTrue(sum < 0, "precondition: the old summed watermark overflows to negative here");

        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", olapTableWithEpochs(epochs));
        assertFalse(snap.isEmpty());
        assertEquals(expectedMax, snap.getSnapshotId());
        assertTrue(snap.getSnapshotId() > 0, "max watermark must stay positive (no overflow)");
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
        // All native OLAP deltas are conservatively RETRACTABLE (DELETE is legal on every KeysType),
        // independent of the table's key type — so a single representative case suffices.
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
        // Models a DROP of the max partition: the MV's last-seen watermark (7) is now greater than
        // the table's current watermark (3). This must surface as the TYPED ancestry exception so
        // MVIVMRefreshProcessor can route it to the "partition-shape change / recreate MV" path.
        OlapTable table = olapTableWithEpochs(3L);
        TvrTableSnapshot from = TvrTableSnapshot.of(7L);
        TvrTableSnapshot to = TvrTableSnapshot.of(3L);
        TvrAncestryBrokenException ex = assertThrows(TvrAncestryBrokenException.class,
                () -> store.listTableDeltaTraits("db", table, from, to));
        assertTrue(ex.getMessage().contains("is not a parent ancestor"));
    }

    @Test
    public void testDeltaTraits_emptyToSnapshot_throwsBadArgumentNotAncestry() {
        // An empty TARGET is a bad-argument error, not a broken ancestry — it must NOT be the typed
        // ancestry exception (otherwise the processor would misroute it as a shape change), and the
        // message must blame the target, matching the Iceberg connector's wording.
        OlapTable table = olapTableWithEpochs(5L);
        TvrTableSnapshot from = TvrTableSnapshot.of(3L);
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> store.listTableDeltaTraits("db", table, from, TvrTableSnapshot.empty()));
        assertFalse(ex instanceof TvrAncestryBrokenException);
        assertTrue(ex.getMessage().contains("toSnapshotInclusive must have a valid snapshot ID"));
    }

    @Test
    public void testDeltaTraits_emptyToWithEmptyFrom_shortCircuitsEmpty() {
        // from == to (both empty) short-circuits to an empty delta list before the target is unwrapped.
        OlapTable table = olapTableWithEpochs(5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.empty());
        assertTrue(traits.isEmpty());
    }

    @Test
    public void testDeltaTraits_typedAncestryExceptionIsConnectorException() {
        // Guards the class hierarchy the processor's catch(StarRocksConnectorException) relies on.
        assertTrue(new TvrAncestryBrokenException("x is not a parent ancestor of y")
                instanceof StarRocksConnectorException);
    }
}
