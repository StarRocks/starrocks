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


package com.starrocks.lake;

import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    NodeMgr nodeMgr;

    @Mocked
    NodeSelector nodeSelector;

    @Test
    public void testChooseBackend() {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                SystemInfoService systemInfo = new SystemInfoService();
                return systemInfo;
            }
        };

        new MockUp<LakeTablet>() {
            @Mock
            public long getPrimaryComputeNodeId(long clusterId) throws UserException {
                throw new UserException("Failed to get primary backend");
            }
        };

        new MockUp<NodeSelector>() {
            @Mock
            public Long seqChooseBackendOrComputeId() throws UserException {
                throw new UserException("No backend or compute node alive.");
            }
        };
    }

    @Test
    public void testGetWarehouse() {
        WarehouseManager manager = new WarehouseManager();
        manager.initDefaultWarehouse();

        long workerGroupId = Utils.getFirstWorkerGroupByWarehouseId(manager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(StarOSAgent.DEFAULT_WORKER_GROUP_ID, workerGroupId);
    }

    @Test
    public void testGetWarehouseIdByBackend() {
        SystemInfoService systemInfo = new SystemInfoService();
        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        b1.setBePort(9060);
        b1.setWarehouseId(10001L);
        Backend b2 = new Backend(10002L, "192.168.0.2", 9050);
        b2.setBePort(9060);
        b2.setWarehouseId(10002L);

        // add two backends to different warehouses
        systemInfo.addBackend(b1);
        systemInfo.addBackend(b2);

        // If the version of be is old, it may pass null.
        long warehouseId = Utils.getWarehouseIdByBackendId(systemInfo, 0);
        Assert.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, warehouseId);

        // pass a wrong tBackend
        warehouseId = Utils.getWarehouseIdByBackendId(systemInfo, 10003);
        Assert.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, warehouseId);

        // pass a right tBackend
        warehouseId = Utils.getWarehouseIdByBackendId(systemInfo, 10001);
        Assert.assertEquals(10001L, warehouseId);
        warehouseId = Utils.getWarehouseIdByBackendId(systemInfo, 10002);
        Assert.assertEquals(10002L, warehouseId);
    }
}
