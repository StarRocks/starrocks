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

package com.starrocks.qe.scheduler;

import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IncrementalDeployOlapTest extends SchedulerTestBase {
    private static final String SQL = "select L_ORDERKEY from lineitem";
    private static final int LINEITEM_TABLETS = 20;

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(false);
        connectContext.getSessionVariable().setEnablePhasedScheduler(false);
    }

    /**
     * OLAP incremental delivery requires a shared-data worker provider and a cloud-native (lake)
     * table; mimic both in the shared-nothing test harness: the provider reports backup-node
     * support (the worker-fungibility signal), and the scan node reports a non-local table
     * (lake tables carry pinned versions inside each scan range).
     */
    private static void mockSharedDataLakeScan() {
        new MockUp<DefaultWorkerProvider>() {
            @Mock
            public boolean allowUsingBackupNode() {
                return true;
            }
        };
        new MockUp<OlapScanNode>() {
            @Mock
            public boolean isLocalNativeTable() {
                return false;
            }
        };
    }

    private List<TExecPlanFragmentParams> captureDeployRequests(int maxScanRangesPerDeploy) {
        List<TExecPlanFragmentParams> requests = new ArrayList<>();
        new MockUp<Deployer>() {
            @Mock
            public void deployFragments(DeployState deployState) {
                for (List<FragmentInstanceExecState> execStates : deployState.getThreeStageExecutionsToDeploy()) {
                    for (FragmentInstanceExecState execState : execStates) {
                        TExecPlanFragmentParams request = execState.getRequestToDeploy();
                        int scanRanges = collectScanRanges(request.params).stream()
                                .mapToInt(p -> p.isEmpty() ? 0 : 1)
                                .sum();
                        Assertions.assertTrue(scanRanges <= maxScanRangesPerDeploy,
                                "deploy carries " + scanRanges + " scan ranges, limit " + maxScanRangesPerDeploy);
                        // instance.node2ScanRanges is shared during incremental deployment.
                        requests.add(request.deepCopy());
                    }
                }
            }
        };
        return requests;
    }

    private static List<TScanRangeParams> collectScanRanges(TPlanFragmentExecParams params) {
        List<TScanRangeParams> res = new ArrayList<>();
        if (params.isSetPer_node_scan_ranges()) {
            params.getPer_node_scan_ranges().values().forEach(res::addAll);
        }
        if (params.isSetNode_to_per_driver_seq_scan_ranges()) {
            params.getNode_to_per_driver_seq_scan_ranges().values()
                    .forEach(perDriverSeq -> perDriverSeq.values().forEach(res::addAll));
        }
        return res;
    }

    private static Set<Long> collectTabletIds(List<TExecPlanFragmentParams> requests) {
        Set<Long> tabletIds = new HashSet<>();
        for (TExecPlanFragmentParams request : requests) {
            for (TScanRangeParams p : collectScanRanges(request.params)) {
                if (!p.isEmpty() && p.getScan_range().isSetInternal_scan_range()) {
                    tabletIds.add(p.getScan_range().getInternal_scan_range().getTablet_id());
                }
            }
        }
        return tabletIds;
    }

    private List<TExecPlanFragmentParams> runIncremental(int batchSize) throws Exception {
        mockSharedDataLakeScan();
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(true);
        connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(batchSize);
        List<TExecPlanFragmentParams> requests = captureDeployRequests(batchSize);
        DefaultCoordinator coordinator = startScheduling(SQL);
        Assertions.assertTrue(coordinator.getJobSpec().isIncrementalScanRanges());
        return requests;
    }

    private void checkIncrementalDelivery(List<TExecPlanFragmentParams> requests) {
        // Every tablet is delivered exactly once across all batches.
        Assertions.assertEquals(LINEITEM_TABLETS, collectTabletIds(requests).size());

        // Per instance and per deploy wave (in deploy order): every wave carrying scan ranges has a
        // sentinel; only the last wave's sentinel has has_more=false, and nothing follows it.
        Map<TUniqueId, List<Boolean>> sentinelHasMorePerWave = new HashMap<>();
        for (TExecPlanFragmentParams request : requests) {
            List<TScanRangeParams> ranges = collectScanRanges(request.params);
            if (ranges.isEmpty()) {
                continue;
            }
            List<TScanRangeParams> sentinels = ranges.stream().filter(TScanRangeParams::isEmpty).toList();
            Assertions.assertEquals(1, sentinels.size(),
                    "each wave must carry exactly one sentinel; instance="
                            + request.params.getFragment_instance_id() + " ranges=" + ranges);
            Assertions.assertTrue(sentinels.get(0).isSetHas_more());
            sentinelHasMorePerWave
                    .computeIfAbsent(request.params.getFragment_instance_id(), k -> new ArrayList<>())
                    .add(sentinels.get(0).isHas_more());
        }
        Assertions.assertFalse(sentinelHasMorePerWave.isEmpty());
        for (Map.Entry<TUniqueId, List<Boolean>> entry : sentinelHasMorePerWave.entrySet()) {
            List<Boolean> waves = entry.getValue();
            for (int i = 0; i < waves.size(); i++) {
                boolean expectedHasMore = i < waves.size() - 1;
                Assertions.assertEquals(expectedHasMore, waves.get(i),
                        "instance " + entry.getKey() + " wave " + i + " of " + waves.size());
            }
        }
    }

    @Test
    public void testIncrementalDeliveryAllAtOnceSchedule() throws Exception {
        List<TExecPlanFragmentParams> requests = runIncremental(4);
        checkIncrementalDelivery(requests);
        // 20 tablets in batches of 4 → more than one deploy round.
        Assertions.assertTrue(requests.size() > 3);
    }

    @Test
    public void testIncrementalDeliveryPhasedSchedule() throws Exception {
        connectContext.getSessionVariable().setEnablePhasedScheduler(true);
        List<TExecPlanFragmentParams> requests = runIncremental(4);
        checkIncrementalDelivery(requests);
    }

    @Test
    public void testSingleBatchWhenBatchSizeExceedsTablets() throws Exception {
        List<TExecPlanFragmentParams> requests = runIncremental(1000);
        checkIncrementalDelivery(requests);
    }

    @Test
    public void testSameTabletsAsNonIncremental() throws Exception {
        List<TExecPlanFragmentParams> incremental = runIncremental(4);
        Set<Long> incrementalTablets = collectTabletIds(incremental);

        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(false);
        List<TExecPlanFragmentParams> plain = captureDeployRequests(Integer.MAX_VALUE);
        startScheduling(SQL);
        Assertions.assertEquals(collectTabletIds(plain), incrementalTablets);
    }

    @Test
    public void testVarOffNoSentinels() throws Exception {
        mockSharedDataLakeScan();
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(false);
        List<TExecPlanFragmentParams> requests = captureDeployRequests(Integer.MAX_VALUE);
        startScheduling(SQL);
        Assertions.assertEquals(LINEITEM_TABLETS, collectTabletIds(requests).size());
        for (TExecPlanFragmentParams request : requests) {
            for (TScanRangeParams p : collectScanRanges(request.params)) {
                Assertions.assertFalse(p.isEmpty(), "no sentinel expected with the variable off");
            }
        }
    }

    @Test
    public void testSharedNothingProviderNotBatched() throws Exception {
        // Variable on, but the worker provider does not support backup nodes (shared-nothing):
        // scans must not batch and no sentinel must appear.
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(true);
        List<TExecPlanFragmentParams> requests = captureDeployRequests(Integer.MAX_VALUE);
        startScheduling(SQL);
        Assertions.assertEquals(LINEITEM_TABLETS, collectTabletIds(requests).size());
        for (TExecPlanFragmentParams request : requests) {
            for (TScanRangeParams p : collectScanRanges(request.params)) {
                Assertions.assertFalse(p.isEmpty(), "no sentinel expected for shared-nothing providers");
            }
        }
    }

    @Test
    public void testColocatedScanNotBatched() throws Exception {
        mockSharedDataLakeScan();
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(true);
        connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(4);
        List<TExecPlanFragmentParams> requests = captureDeployRequests(Integer.MAX_VALUE);
        startScheduling("select count(1) from lineitem0 a join lineitem_partition b on a.L_ORDERKEY = b.L_ORDERKEY");
        Assertions.assertFalse(requests.isEmpty());
        for (TExecPlanFragmentParams request : requests) {
            for (TScanRangeParams p : collectScanRanges(request.params)) {
                Assertions.assertFalse(p.isEmpty(), "colocated scans must not emit sentinels");
            }
        }
    }
}
