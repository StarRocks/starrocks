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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.HiveTable;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.LocationRestrictedWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone preferred-worker assignment checks that do not require GlobalStateMgr warehouse init.
 */
public class HDFSBackendSelectorPreferredWorkersTest {
    @Mocked
    private HdfsScanNode hdfsScanNode;
    @Mocked
    private HiveTable hiveTable;
    @Mocked
    private ConnectContext context;

    private final int scanNodeId = 0;
    private final int computeNodePort = 9030;
    private final String hostFormat = "192.168.1.%02d";

    private List<TScanRangeLocations> createScanRanges(long number, long size) {
        List<TScanRangeLocations> ans = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            ans.add(scanRangeLocations);

            TScanRange scanRange = new TScanRange();
            scanRangeLocations.scan_range = scanRange;
            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            scanRange.hdfs_scan_range = hdfsScanRange;
            hdfsScanRange.setRelative_path(String.format("%06d", i));
            hdfsScanRange.setOffset(0);
            hdfsScanRange.setLength(size);

            List<TScanRangeLocation> locations = new ArrayList<>();
            TScanRangeLocation location = new TScanRangeLocation();
            location.setServer(new TNetworkAddress("localhost", -1));
            locations.add(location);
            scanRangeLocations.setLocations(locations);
        }
        return ans;
    }

    private ImmutableMap<Long, ComputeNode> createComputeNodes(int number) {
        Map<Long, ComputeNode> ans = new HashMap<>();
        for (int i = 0; i < number; i++) {
            ComputeNode node = new ComputeNode(i, String.format(hostFormat, i), computeNodePort);
            node.setBePort(computeNodePort);
            node.setAlive(true);
            ans.put((long) i, node);
        }
        return ImmutableMap.copyOf(ans);
    }

    private Map<Long, Long> computeWorkerIdToReadBytes(FragmentScanRangeAssignment assignment, int scanNodeId) {
        Map<Long, Long> stats = new HashMap<>();
        for (Map.Entry<Long, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            List<TScanRangeParams> scanRangeParams = entry.getValue().get(scanNodeId);
            for (TScanRangeParams params : scanRangeParams) {
                THdfsScanRange scanRange = params.scan_range.hdfs_scan_range;
                stats.put(entry.getKey(), stats.getOrDefault(entry.getKey(), 0L) + scanRange.getLength());
            }
        }
        return stats;
    }

    @Test
    public void testPreferredConnectorWorkersOnly() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        // Avoid CandidateWorkerProvider path which needs a fully initialized GlobalStateMgr.
        sessionVariable.setEnableDataCacheSharing(false);
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;
                hdfsScanNode.getTableName();
                result = "hive_tbl";
                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";
                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        int scanRangeNumber = 100;
        int scanRangeSize = 10000;
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider base = new DefaultWorkerProvider(
                ImmutableMap.of(), computeNodes, ImmutableMap.of(), computeNodes,
                true, WarehouseManager.DEFAULT_RESOURCE);
        // Pool = all 3 workers; connector preferred = only 0 and 1.
        WorkerProvider workerProvider = new LocationRestrictedWorkerProvider(
                base, Set.of(0L, 1L, 2L), Set.of(0L, 1L));

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider,
                        false, false, false, context);
        selector.computeScanRangeAssignment();

        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        Assertions.assertFalse(stats.containsKey(2L), "worker 2 must not receive connector splits");
        Assertions.assertEquals(Set.of(0L, 1L), stats.keySet());
        long total = stats.values().stream().mapToLong(Long::longValue).sum();
        Assertions.assertEquals((long) scanRangeNumber * scanRangeSize, total);
    }
}
