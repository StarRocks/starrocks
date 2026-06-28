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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.assertHookDoesNotDelegate;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Detection-side coverage for {@link BrokerLoadPreSplitHook}: each early-return
 * branch is exercised and asserted via {@code MockedStatic} to never reach
 * {@link TabletPreSplitCoordinator#submitAsynchronously}. The eligible-
 * delegation path needs a full FE fixture (catalog, tablet inverted index,
 * compute-resource warehouse) and is left to integration coverage.
 */
public class BrokerLoadPreSplitHookTest {

    private static final long BASE_INDEX_META_ID = 200L;

    private boolean savedConfigBrokerLoad;

    @BeforeEach
    public void setUp() {
        savedConfigBrokerLoad = Config.enable_tablet_pre_split_for_broker_load;
        Config.enable_tablet_pre_split_for_broker_load = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_tablet_pre_split_for_broker_load = savedConfigBrokerLoad;
    }

    @Test
    public void testConfigFlagOffShortCircuits() throws Exception {
        // Cluster-wide opt-out must short-circuit before the coordinator AND
        // record the eligibility-skip counter under disabled_by_config — the
        // hook returns ahead of the coordinator, so checkConfigAndSession would
        // otherwise never bump the bucket.
        Config.enable_tablet_pre_split_for_broker_load = false;
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_CONFIG.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(singlePartitionOlapTable(), List.of(), List.of()));

            org.junit.jupiter.api.Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "config opt-out must bump the disabled_by_config bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testSessionOptOutShortCircuits() throws Exception {
        // SET enable_tablet_pre_split=false on the session must short-circuit
        // before the eligibility-target walk AND record the eligibility-skip
        // counter under disabled_by_session. The hook now takes the
        // ConnectContext directly (parameter-threaded), so we
        // pass an opted-out context rather than stubbing a static.
        ConnectContext optedOutContext = mockConnectContextWithSessionPreSplit(false);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_SESSION.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(optedOutContext, singlePartitionOlapTable(), List.of(), List.of()));

            org.junit.jupiter.api.Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "session opt-out must bump the disabled_by_session bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testNullFileGroupsShortCircuits() throws Exception {
        assertHookDoesNotDelegate(() ->
                invokeHook(singlePartitionOlapTable(), null, List.of()));
    }

    @Test
    public void testNullFileStatusesShortCircuits() throws Exception {
        assertHookDoesNotDelegate(() ->
                invokeHook(singlePartitionOlapTable(), List.of(), null));
    }

    @Test
    public void testMultiPartitionOlapTableShortCircuits() throws Exception {
        OlapTable target = mock(OlapTable.class);
        when(target.getPhysicalPartitions()).thenReturn(
                List.of(mock(PhysicalPartition.class), mock(PhysicalPartition.class)));

        assertHookDoesNotDelegate(() ->
                invokeHook(target, List.of(mock(BrokerFileGroup.class)), List.of()));
    }

    @Test
    public void testSinglePartitionWithMultipleBaseTabletsRecordsSkip() throws Exception {
        // Table clears the table-level gate but its single base index already
        // holds multiple tablets (the re-load-after-split case). The hook must
        // record multiple_base_index_tablets and not delegate — the
        // coordinator's maybeAct, which would otherwise record it, is never
        // reached on the single-partition resolve-failure path.
        OlapTable target = tablePassingTableLevelGate();
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class), mock(Tablet.class)));
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        when(target.getPhysicalPartitions()).thenReturn(List.of(partition));

        assertSinglePartitionResolveRecordsSkip(target, SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
    }

    @Test
    public void testMissingBaseIndexRecordsSkip() throws Exception {
        // Table clears the table-level gate but the base-index lookup returns
        // null — e.g. an alter changed the base-index id mid-load. The hook
        // must record metadata_not_resolved and not delegate.
        OlapTable target = tablePassingTableLevelGate();
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);
        when(target.getPhysicalPartitions()).thenReturn(List.of(partition));

        assertSinglePartitionResolveRecordsSkip(target, SkipReason.METADATA_NOT_RESOLVED);
    }

    @Test
    public void testInternalThrowIsSwallowed() throws Exception {
        // Drive the outer try/catch by passing an OlapTable whose accessor
        // throws. The hook must not let the throw escape — Broker Load would
        // otherwise abort an already-running pending-task callback.
        OlapTable target = mock(OlapTable.class);
        when(target.getPhysicalPartitions()).thenThrow(new RuntimeException("simulated table failure"));

        assertHookDoesNotDelegate(() ->
                invokeHook(target, List.of(mock(BrokerFileGroup.class)), List.of()));
    }

    /**
     * Invokes {@code BrokerLoadPreSplitHook.maybeRunPreSplit} with default
     * mocks for {@code Database}, {@code BrokerDesc}, and {@code ComputeResource}
     * — none of which the early-return branches consult. Tests pass distinct
     * arguments for the three fields the hook actually inspects on the
     * short-circuit paths: target table, file groups, file statuses. The
     * default context has {@code enable_tablet_pre_split=true} so the
     * session opt-out branch is NOT taken.
     */
    private static void invokeHook(
            OlapTable target, List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses) {
        invokeHook(mockConnectContextWithSessionPreSplit(true), target, fileGroups, fileStatuses);
    }

    /**
     * Overload for tests that need to drive a specific {@link ConnectContext}
     * (e.g. the session opt-out test).
     */
    private static void invokeHook(
            ConnectContext context, OlapTable target,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses) {
        BrokerLoadPreSplitHook.maybeRunPreSplit(
                context, mock(Database.class), target, mock(BrokerDesc.class),
                fileGroups, fileStatuses, mock(ComputeResource.class), () -> false);
    }

    private static OlapTable singlePartitionOlapTable() {
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class)));
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.getPhysicalPartitions()).thenReturn(List.of(partition));
        return table;
    }

    /**
     * Builds an unpartitioned {@link OlapTable} that clears the table-level
     * eligibility gate ({@link PreSplitTargets#findEligibleTable}) so the hook
     * proceeds into the single-partition flow. Per-partition shape (physical
     * partitions, base index, tablets) is left to the caller to stub. The
     * sort-key column is supplied by {@link #assertSinglePartitionResolveRecordsSkip}
     * via a {@code MockedStatic<MetaUtils>}.
     */
    private static OlapTable tablePassingTableLevelGate() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(mock(MaterializedIndexMeta.class)));
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(false);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        return table;
    }

    /**
     * Invokes the hook against a table that passes the table-level gate but
     * fails single-partition target resolution, and asserts it (a) never
     * delegates to the coordinator and (b) bumps the {@code eligibility_skipped}
     * counter under {@code expectedReason} exactly once. A
     * {@code MockedStatic<MetaUtils>} supplies a scalar sort key so the
     * table-level gate's sort-key check passes.
     */
    private static void assertSinglePartitionResolveRecordsSkip(
            OlapTable target, SkipReason expectedReason) throws Exception {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target))
                    .thenReturn(List.of(PresplitTestSupport.bigintColumn("k")));
            String label = expectedReason.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(target, List.of(mock(BrokerFileGroup.class)),
                            List.of(List.<TBrokerFileStatus>of())));

            Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                            .getMetric(label).getValue().longValue(),
                    "single-partition resolve failure must bump the " + label + " bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

}
