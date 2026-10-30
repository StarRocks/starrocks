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

package com.starrocks.planner;

import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNodeInfo;
import com.starrocks.thrift.TPlanNode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FetchNodeTest {

    // In shared-nothing mode an external table scan can run on a compute node, so the lookup request must be able
    // to reach compute nodes as well as backends.
    @Test
    public void testNodesInfoIncludesComputeNodesInSharedNothing() {
        SystemInfoService service = new SystemInfoService();
        service.addBackend(new Backend(10001, "127.0.0.1", 9050));
        service.addComputeNode(new ComputeNode(25011, "127.0.0.2", 9050));
        service.addComputeNode(new ComputeNode(25012, "127.0.0.3", 9050));

        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return false;
            }
        };
        new MockUp<NodeMgr>() {
            @Mock
            SystemInfoService getClusterInfo() {
                return service;
            }
        };

        EmptySetNode input = new EmptySetNode(new PlanNodeId(0), new ArrayList<>(List.of(new TupleId(0))));
        FetchNode fetchNode = new FetchNode(new PlanNodeId(1), input, new PlanNodeId(2), new ArrayList<>(),
                new HashMap<>(), WarehouseManager.DEFAULT_RESOURCE);
        TPlanNode msg = new TPlanNode();
        fetchNode.toThrift(msg);

        Set<Long> ids = msg.fetch_node.nodes_info.getNodes().stream().map(TNodeInfo::getId).collect(Collectors.toSet());
        assertEquals(Set.of(10001L, 25011L, 25012L), ids);
    }
}
