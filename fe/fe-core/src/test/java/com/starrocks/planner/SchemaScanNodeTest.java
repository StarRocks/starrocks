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

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SchemaScanNodeTest {
    @Mocked
    private ConnectContext connectContext;

    public SchemaScanNodeTest() {
        connectContext = new ConnectContext(null);
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testComputeFeNodes(@Mocked GlobalStateMgr globalStateMgr) {
        List<Frontend> frontends = new ArrayList<>();
        frontends.add(new Frontend());
        Frontend frontend = new Frontend();
        frontend.setAlive(true);
        frontends.add(frontend);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getNodeMgr().getFrontends(null);
                minTimes = 0;
                result = frontends;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        SystemTable table = new SystemTable(0, "fe_metrics", null, null, null);
        desc.setTable(table);
        SchemaScanNode scanNode = new SchemaScanNode(new PlanNodeId(0), desc);

        scanNode.computeFeNodes();

        Assert.assertNotNull(scanNode.getFrontends());
    }

    @Test
    public void testComputeBeScanRanges() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        UtFrameUtils.mockInitWarehouseEnv();

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                ComputeNode computeNode = new ComputeNode(1L, "127.0.0.1", 9030);
                computeNode.setAlive(true);
                return computeNode;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        SystemTable table = new SystemTable(0, "fe_metrics", null, null, null);
        desc.setTable(table);
        SchemaScanNode scanNode = new SchemaScanNode(new PlanNodeId(0), desc);
        scanNode.computeBeScanRanges();
    }

    @Test
    public void testComputeNodeScanRanges() {
        new MockUp<SystemInfoService>() {
            @Mock
            public List<ComputeNode> getComputeNodes() {
                ComputeNode computeNode = new ComputeNode(1L, "127.0.0.1", 9030);
                computeNode.setAlive(true);
                return List.of(computeNode);
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        SystemTable table = new SystemTable(0, "be_datacache_metrics", null, null, null);
        desc.setTable(table);
        SchemaScanNode scanNode = new SchemaScanNode(new PlanNodeId(0), desc);
        scanNode.computeBeScanRanges();
        Assert.assertEquals(1, scanNode.getScanRangeLocations(0).size());
    }
}