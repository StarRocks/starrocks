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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/proc/BackendsProcDirTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.DefaultWarehouse;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BackendsProcDirTest {
    private Backend b1;
    private Backend b2;
    private final long tabletNumSharedData = 200;
    private final long tabletNumSharedNothing = 2;

    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    private TabletInvertedIndex tabletInvertedIndex;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private StarOSAgent starOsAgent;
    @Mocked
    private RunMode runMode;

    @Mocked
    private NodeMgr nodeMgr;

    private final VariableMgr variableMgr = new VariableMgr();

    public BackendsProcDirTest() {
    }

    @Before
    public void setUp() {
        b1 = new Backend(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        b2 = new Backend(1001, "host2", 20000);
        b2.updateOnce(20001, 20003, 20005);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = 10000L;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.clear();
                minTimes = 0;

                systemInfoService.getBackend(1000);
                minTimes = 0;
                result = b1;

                systemInfoService.getBackend(1001);
                minTimes = 0;
                result = b2;

                systemInfoService.getBackend(1002);
                minTimes = 0;
                result = null;

                tabletInvertedIndex.getTabletNumByBackendId(anyLong);
                minTimes = 0;
                result = tabletNumSharedNothing;

                starOsAgent.getWorkerTabletNum(anyString);
                minTimes = 0;
                result = tabletNumSharedData;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getTabletInvertedIndex();
                minTimes = 0;
                result = tabletInvertedIndex;

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

    @Test
    public void testLookupNormal() {
        ExceptionChecker.expectThrowsNoException(() -> {
            BackendsProcDir dir = new BackendsProcDir(systemInfoService);
            ProcNodeInterface node = dir.lookup("1000");
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof BackendProcNode);
        });

        ExceptionChecker.expectThrowsNoException(() -> {
            BackendsProcDir dir = new BackendsProcDir(systemInfoService);
            ProcNodeInterface node = dir.lookup("1001");
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof BackendProcNode);
        });

        ExceptionChecker.expectThrows(AnalysisException.class, () -> {
            BackendsProcDir dir = new BackendsProcDir(systemInfoService);
            dir.lookup("1002");
        });
    }

    @Test
    public void testLookupInvalid() {
        BackendsProcDir dir = new BackendsProcDir(systemInfoService);
        ExceptionChecker.expectThrows(AnalysisException.class, () -> dir.lookup(null));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> dir.lookup(""));
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
                runMode.isSharedDataMode();
                minTimes = 0;
                result = false;
            }
        };

        BackendsProcDir dir = new BackendsProcDir(systemInfoService);
        ProcResult result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);
        int columnIndex = getTabletNumColumnIndex(result.getColumnNames());
        Assert.assertTrue(columnIndex >= 0);
        for (List<String> row : result.getRows()) {
            Assert.assertEquals(String.valueOf(tabletNumSharedNothing), row.get(columnIndex));
        }
    }

    @Test
    public void testFetchResultSharedData() throws AnalysisException {
        new Expectations() {
            {
                RunMode.isSharedDataMode();
                minTimes = 0;
                result = true;
            }
        };

        BackendsProcDir dir = new BackendsProcDir(systemInfoService);
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
    public void testIPTitle() {
        Assert.assertEquals("IP", BackendsProcDir.TITLE_NAMES.get(1));
    }

    @Test
    public void testWarehouse(@Mocked WarehouseManager warehouseManager) throws AnalysisException {
        new Expectations() {
            {
                systemInfoService.getBackendIds(anyBoolean);
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

        BackendsProcDir dir = new BackendsProcDir(systemInfoService);
        ProcResult result = dir.fetchResult();
    }
}
