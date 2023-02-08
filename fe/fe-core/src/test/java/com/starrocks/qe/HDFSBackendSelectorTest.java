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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.HiveTable;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HDFSBackendSelectorTest {
    @Mocked
    private HdfsScanNode hdfsScanNode;
    @Mocked
    private HiveTable hiveTable;
    final int scanNodeId = 0;
    final int computeNodePort = 9030;
    final String hostFormat = "Host%02d";

    private List<TScanRangeLocations> createScanRanges(int number, int size) {
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

    private List<ComputeNode> createComputeNodes(int number) {
        List<ComputeNode> ans = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            ComputeNode node = new ComputeNode(i, String.format(hostFormat, i), computeNodePort);
            node.setBePort(computeNodePort);
            node.setAlive(true);
            ans.add(node);
        }
        return ans;
    }

    private Map<TNetworkAddress, Long> computeHostReadBytes(
            FragmentScanRangeAssignment assignment,
            int scanNodeId) {
        Map<TNetworkAddress, Long> stats = new HashMap<>();
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
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
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment =
                new FragmentScanRangeAssignment();
        Map<TNetworkAddress, Long> addressToBackendId = new HashMap<>();
        Set<Long> usedBackendIDs = new HashSet<>();
        List<ComputeNode> computeNodes = createComputeNodes(hostNumber);

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, addressToBackendId, usedBackendIDs,
                        ImmutableList.copyOf(computeNodes), false, false);
        selector.computeScanRangeAssignment();

        int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        int variance = 5 * scanRangeSize;
        Map<TNetworkAddress, Long> stats = computeHostReadBytes(assignment, scanNodeId);
        for (Map.Entry<TNetworkAddress, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
            Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        }
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

        FragmentScanRangeAssignment assignment =
                new FragmentScanRangeAssignment();
        Map<TNetworkAddress, Long> addressToBackendId = new HashMap<>();
        Set<Long> usedBackendIDs = new HashSet<>();
        List<ComputeNode> computeNodes = createComputeNodes(hostNumber);

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, addressToBackendId, usedBackendIDs,
                        ImmutableList.copyOf(computeNodes), true, true);
        selector.computeScanRangeAssignment();

        Map<TNetworkAddress, Long> stats = computeHostReadBytes(assignment, scanNodeId);
        Assert.assertEquals(stats.size(), localHostNumber);
        for (Map.Entry<TNetworkAddress, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
        }
    }
}
