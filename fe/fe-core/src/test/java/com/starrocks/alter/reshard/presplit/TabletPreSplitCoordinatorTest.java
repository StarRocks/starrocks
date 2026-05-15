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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TabletPreSplitCoordinatorTest {

    private static final long PARTITION_ID = 10001L;
    private static final long BASE_INDEX_META_ID = 200L;

    private Database database;
    private OlapTable table;
    private PhysicalPartition partition;
    private MaterializedIndex baseIndex;

    @BeforeEach
    public void setUp() {
        Config.enable_tablet_pre_split_for_insert_from_files = true;
        Config.enable_tablet_pre_split_for_broker_load = false;
        // Pin tablet-count-selection inputs so the test arithmetic stays valid if defaults move.
        Config.tablet_reshard_target_size = 10L * DebugUtil.GIGABYTE;
        Config.tablet_reshard_max_split_count = 1024;

        // Bind a fresh ConnectContext so the coordinator's session-var check finds one.
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableTabletPreSplit(true);
        connectContext.setSessionVariable(sessionVariable);
        connectContext.setThreadLocalInfo();

        database = mock(Database.class);
        baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class)));
        when(baseIndex.getRowCount()).thenReturn(0L);

        partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);

        table = mock(OlapTable.class);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(mock(MaterializedIndexMeta.class)));
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.getPhysicalPartition(PARTITION_ID)).thenReturn(partition);
        Column scalarKey = mock(Column.class);
        Type scalarType = IntegerType.BIGINT;
        when(scalarKey.getType()).thenReturn(scalarType);
        when(table.getKeyColumnsInOrder()).thenReturn(List.of(scalarKey));
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private PreSplitOutcome invokeMaybeAct() {
        return TabletPreSplitCoordinator.maybeAct(database, table, PARTITION_ID, DUMMY_CONTEXT);
    }

    private static void assertSkipped(PreSplitOutcome outcome, SkipReason expected) {
        Assertions.assertInstanceOf(PreSplitOutcome.Skipped.class, outcome,
                "expected Skipped(" + expected + "), got: " + outcome);
        Assertions.assertEquals(expected, ((PreSplitOutcome.Skipped) outcome).reason());
    }

    @Test
    public void testHappyPathReturnsEligible() {
        Assertions.assertInstanceOf(PreSplitOutcome.Eligible.class, invokeMaybeAct());
    }

    @Test
    public void testBothConfigsOffSkipped() {
        Config.enable_tablet_pre_split_for_insert_from_files = false;
        Config.enable_tablet_pre_split_for_broker_load = false;

        assertSkipped(invokeMaybeAct(), SkipReason.DISABLED_BY_CONFIG);
    }

    @Test
    public void testBrokerConfigAloneReturnsEligible() {
        Config.enable_tablet_pre_split_for_insert_from_files = false;
        Config.enable_tablet_pre_split_for_broker_load = true;

        Assertions.assertInstanceOf(PreSplitOutcome.Eligible.class, invokeMaybeAct());
    }

    @Test
    public void testSessionVariableOffSkipped() {
        ConnectContext.get().getSessionVariable().setEnableTabletPreSplit(false);

        assertSkipped(invokeMaybeAct(), SkipReason.DISABLED_BY_SESSION);
    }

    @Test
    public void testNotRangeDistributionSkipped() {
        when(table.isRangeDistribution()).thenReturn(false);

        assertSkipped(invokeMaybeAct(), SkipReason.NOT_RANGE_DISTRIBUTION);
    }

    @Test
    public void testNonNormalTableSkipped() {
        when(table.getState()).thenReturn(OlapTable.OlapTableState.SCHEMA_CHANGE);

        assertSkipped(invokeMaybeAct(), SkipReason.TABLE_NOT_NORMAL);
    }

    @Test
    public void testMaterializedViewOrRollupSkipped() {
        // visibleIndexMetas.size() > 1 means at least one MV or rollup is attached.
        when(table.getVisibleIndexMetas()).thenReturn(
                List.of(mock(MaterializedIndexMeta.class), mock(MaterializedIndexMeta.class)));

        assertSkipped(invokeMaybeAct(), SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP);
    }

    @Test
    public void testUnsupportedSortKeyColumnTypeSkipped() {
        Column nonScalarKey = mock(Column.class);
        Type nonScalarType = mock(Type.class);
        when(nonScalarType.isScalarType()).thenReturn(false);
        when(nonScalarKey.getType()).thenReturn(nonScalarType);
        when(table.getKeyColumnsInOrder()).thenReturn(List.of(nonScalarKey));

        assertSkipped(invokeMaybeAct(), SkipReason.UNSUPPORTED_SORT_KEY);
    }

    @Test
    public void testEmptyKeyColumnsSkipped() {
        when(table.getKeyColumnsInOrder()).thenReturn(List.of());

        assertSkipped(invokeMaybeAct(), SkipReason.UNSUPPORTED_SORT_KEY);
    }

    @Test
    public void testMultipleBaseIndexTabletsSkipped() {
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class), mock(Tablet.class)));

        assertSkipped(invokeMaybeAct(), SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
    }

    @Test
    public void testPartitionNotEmptySkipped() {
        when(baseIndex.getRowCount()).thenReturn(42L);

        assertSkipped(invokeMaybeAct(), SkipReason.PARTITION_NOT_EMPTY);
    }

    @Test
    public void testMissingPartitionSkipped() {
        when(table.getPhysicalPartition(PARTITION_ID)).thenReturn(null);

        assertSkipped(invokeMaybeAct(), SkipReason.METADATA_NOT_RESOLVED);
    }

    @Test
    public void testMissingBaseIndexSkipped() {
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);

        assertSkipped(invokeMaybeAct(), SkipReason.METADATA_NOT_RESOLVED);
    }

    // ---- selectTabletCount (B2) ----

    private static int selectTabletCount(long totalBytes, int activeComputeNodeCount) {
        return TabletPreSplitCoordinator.selectTabletCount(
                new Estimates(totalBytes, /*totalRows*/ 0L), activeComputeNodeCount);
    }

    @Test
    public void testTabletCountSmallLoadOnThreeComputeNodes() {
        // 1 GB / 10 GB target rounds up to 1 tablet by bytes; compute-node floor of 3 wins.
        Assertions.assertEquals(3, selectTabletCount(DebugUtil.GIGABYTE, 3));
    }

    @Test
    public void testTabletCountLargeLoadOnThreeComputeNodes() {
        // 100 GB / 10 GB target = 10 tablets by bytes; beats the compute-node floor of 3.
        Assertions.assertEquals(10, selectTabletCount(100L * DebugUtil.GIGABYTE, 3));
    }

    @Test
    public void testTabletCountSmallLoadOnTwelveComputeNodes() {
        Assertions.assertEquals(12, selectTabletCount(DebugUtil.GIGABYTE, 12));
    }

    @Test
    public void testTabletCountLargeLoadOnTwelveComputeNodes() {
        // 100 GB / 10 GB target = 10 tablets by bytes; compute-node floor of 12 wins.
        Assertions.assertEquals(12, selectTabletCount(100L * DebugUtil.GIGABYTE, 12));
    }

    @Test
    public void testTabletCountSaturatesAtMaxSplitCount() {
        // 10 PB / 10 GB target ≈ 1M tablets by bytes; clamps to tablet_reshard_max_split_count.
        Assertions.assertEquals(Config.tablet_reshard_max_split_count,
                selectTabletCount(10L * 1024L * DebugUtil.TERABYTE, 1));
    }

    @Test
    public void testTabletCountFloorsAtTwo() {
        // Zero bytes + single compute node would give 1; clamp lifts it to the minimum 2.
        Assertions.assertEquals(2, selectTabletCount(0L, 1));
    }

    @Test
    public void testTabletCountRejectsNonPositiveComputeNodeCount() {
        Estimates anyEstimates = new Estimates(DebugUtil.GIGABYTE, 0L);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TabletPreSplitCoordinator.selectTabletCount(anyEstimates, -1));
    }
}
