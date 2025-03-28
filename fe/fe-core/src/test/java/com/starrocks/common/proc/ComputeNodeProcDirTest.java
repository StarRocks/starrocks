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

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.DefaultWarehouse;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ComputeNodeProcDirTest {
    private ComputeNode b1;
    private ComputeNode b2;
    private final long tabletNumSharedData = 200;

    private final SystemInfoService systemInfoService = new SystemInfoService();

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private StarOSAgent starOsAgent;
    @Mocked
    private NodeMgr nodeMgr;
    @Mocked
    private RunMode runMode;

    private final VariableMgr variableMgr = new VariableMgr();

    @Before
    public void setUp() {
        b1 = new ComputeNode(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        b2 = new Backend(1001, "host2", 20000);
        b2.updateOnce(20001, 20003, 20005);
        systemInfoService.addComputeNode(b1);
        systemInfoService.addComputeNode(b2);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                starOsAgent.getWorkerTabletNum(anyString);
                minTimes = 0;
                result = tabletNumSharedData;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starOsAgent;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

    }

    @After
    public void tearDown() {
        // systemInfoService = null;
    }

    private int getTabletNumColumnIndex(List<String> names) {
        for (int i = 0; i < names.size(); ++i) {
            if ("TabletNum".equals(names.get(i))) {
                return i;
            }
        }
        return -1;
    }

    @Test
    public void testFetchResultSharedNothing() throws AnalysisException {
        new Expectations() {
            {
                RunMode.isSharedDataMode();
                minTimes = 0;
                result = false;
            }
        };

        ComputeNodeProcDir dir = new ComputeNodeProcDir(systemInfoService);
        ProcResult result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);
        int columnIndex = getTabletNumColumnIndex(result.getColumnNames());
        // no "TabletNum" column in shared-nothing mode
        Assert.assertEquals(-1, columnIndex);
    }

    @Test
    public void testFetchResultSharedData(@Mocked WarehouseManager warehouseManager) throws AnalysisException {
        new Expectations() {
            {
                RunMode.isSharedDataMode();
                minTimes = 1;
                result = true;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        ComputeNodeProcDir dir = new ComputeNodeProcDir(systemInfoService);
        ProcResult result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);
        int columnIndex = getTabletNumColumnIndex(result.getColumnNames());
        Assert.assertTrue(columnIndex >= 0);
        for (List<String> row : result.getRows()) {
            Assert.assertEquals(String.valueOf(tabletNumSharedData), row.get(columnIndex));
        }
    }

    @Test
    public void testWarehouse(@Mocked WarehouseManager warehouseManager) throws AnalysisException {
        new Expectations() {
            {
                systemInfoService.getComputeNodeIds(anyBoolean);
                result = Lists.newArrayList(1000L, 1001L);
            }
        };

        new Expectations() {
            {
                RunMode.isSharedDataMode();
                minTimes = 0;
                result = true;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        ComputeNodeProcDir dir = new ComputeNodeProcDir(systemInfoService);
        ProcResult result = dir.fetchResult();
    }
}
