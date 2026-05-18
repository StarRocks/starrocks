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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
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
        Config.enable_tablet_pre_split_for_broker_load = false;
        assertHookDoesNotDelegate(() ->
                invokeHook(singlePartitionOlapTable(), List.of(), List.of()));
    }

    @Test
    public void testSessionOptOutShortCircuits() throws Exception {
        // SET enable_tablet_pre_split=false on the session must short-circuit
        // before the eligibility-target walk AND record the eligibility-skip
        // counter under disabled_by_session. The hook reads ConnectContext.get()
        // (BrokerLoadJob binds the load's ConnectContext via context.bindScope()
        // around this call in production), so stub the thread-local lookup here.
        // Resolve the inner context mock before the outer mockStatic.when() —
        // Mockito's per-thread stubbing state does not tolerate nested
        // mock()/when() inside another when() argument.
        ConnectContext optedOutContext = mockConnectContextWithSessionPreSplit(false);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_SESSION.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            try (MockedStatic<ConnectContext> contextStatic = Mockito.mockStatic(ConnectContext.class)) {
                contextStatic.when(ConnectContext::get).thenReturn(optedOutContext);
                assertHookDoesNotDelegate(() ->
                        invokeHook(singlePartitionOlapTable(), List.of(), List.of()));
            }

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
    public void testSinglePartitionWithMultipleBaseTabletsShortCircuits() throws Exception {
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class), mock(Tablet.class)));

        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);

        OlapTable target = mock(OlapTable.class);
        when(target.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(target.getPhysicalPartitions()).thenReturn(List.of(partition));

        assertHookDoesNotDelegate(() ->
                invokeHook(target, List.of(mock(BrokerFileGroup.class)), List.of()));
    }

    @Test
    public void testMissingBaseIndexShortCircuits() throws Exception {
        // Partition exists but the base-index lookup returns null — e.g. an
        // alter changed the base-index id mid-load. Hook must skip.
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);

        OlapTable target = mock(OlapTable.class);
        when(target.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(target.getPhysicalPartitions()).thenReturn(List.of(partition));

        assertHookDoesNotDelegate(() ->
                invokeHook(target, List.of(mock(BrokerFileGroup.class)), List.of()));
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
     * short-circuit paths: target table, file groups, file statuses.
     */
    private static void invokeHook(
            OlapTable target, List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses) {
        BrokerLoadPreSplitHook.maybeRunPreSplit(
                mock(Database.class), target, mock(BrokerDesc.class),
                fileGroups, fileStatuses, mock(ComputeResource.class));
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

}
