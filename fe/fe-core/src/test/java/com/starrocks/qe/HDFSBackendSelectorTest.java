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
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSBackendSelectorTest {
    @Mocked
    private HdfsScanNode hdfsScanNode;
    @Mocked
    private HiveTable hiveTable;
    @Mocked
    private ConnectContext context;
    final int scanNodeId = 0;
    final int computeNodePort = 9030;
    final String hostFormat = "Host%02d";

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
    public void testHdfsScanNodeHashRing() throws Exception {
        new MockUp<PlannerProfile>() {
            @Mock
            public void addCustomProperties(String name, String value) {
            }
        };
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                ConnectContext.get();
                result = context;

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
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, false, false);
        selector.computeScanRangeAssignment();

        int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        int variance = 5 * scanRangeSize;
        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
            Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        }
    }

    @Test
    public void testHdfsScanNodeScanRangeReBalance() throws Exception {
        new MockUp<PlannerProfile>() {
            @Mock
            public void addCustomProperties(String name, String value) {
            }
        };
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                ConnectContext.get();
                result = context;

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        long scanRangeNumber = 100;
        long scanRangeSize = 10000;
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, false, false);
        selector.computeScanRangeAssignment();

        long avg = (scanRangeNumber * scanRangeSize) / hostNumber + 1;
        double variance = 0.2 * avg;
        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
            Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        }

        variance = 2 * scanRangeSize;
        for (Map.Entry<ComputeNode, Long> entry : selector.reBalanceBytesPerComputeNode.entrySet()) {
            System.out.printf("%s -> %d bytes re-balance\n", entry.getKey(), entry.getValue());
            Assert.assertTrue(entry.getValue() <= variance);
        }
    }

    @Test
    public void testHashRingAlgorithm() {
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                ConnectContext.get();
                result = context;

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
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );
        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, false, false);
        HashRing hashRing = selector.makeHashRing();
        Assert.assertTrue(hashRing.policy().equals("ConsistentHash"));
        ConsistentHashRing consistentHashRing = (ConsistentHashRing) hashRing;
        Assert.assertTrue(consistentHashRing.getVirtualNumber() ==
                HDFSBackendSelector.CONSISTENT_HASH_RING_VIRTUAL_NUMBER);

        sessionVariable.setHdfsBackendSelectorHashAlgorithm("rendezvous");
        hashRing = selector.makeHashRing();
        Assert.assertTrue(hashRing.policy().equals("RendezvousHash"));

        sessionVariable.setHdfsBackendSelectorHashAlgorithm("consistent");
        sessionVariable.setConsistentHashVirtualNodeNum(64);
        hashRing = selector.makeHashRing();
        Assert.assertTrue(hashRing.policy().equals("ConsistentHash"));
        consistentHashRing = (ConsistentHashRing) hashRing;
        Assert.assertTrue(consistentHashRing.getVirtualNumber() == 64);
    }

    @Test
    public void testHdfsScanNodeForceScheduleLocal() throws Exception {
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;
                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";
            }
        };

        int scanRangeNumber = 100;
        int scanRangeSize = 10000;
        int hostNumber = 100;

        // rewrite scan ranges locations to only 3 hosts.
        // so with `forceScheduleLocal` only 3 nodes will get scan ranges.
        int localHostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        for (TScanRangeLocations location : locations) {
            List<TScanRangeLocation> servers = location.locations;
            servers.clear();
            for (int i = 0; i < localHostNumber; i++) {
                TScanRangeLocation loc = new TScanRangeLocation();
                loc.setServer(new TNetworkAddress(String.format(hostFormat, i), computeNodePort));
                servers.add(loc);
            }
        }

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, true, false);
        selector.computeScanRangeAssignment();

        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        Assert.assertEquals(stats.size(), localHostNumber);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
        }
    }
}
