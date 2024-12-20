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

package com.starrocks.warehouse;

import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import org.junit.Assert;
import org.junit.Test;

public class WarehouseIdleCheckerTest {

    @Test
    public void getStatus() {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.initDefaultWarehouse();

        warehouseManager.addWarehouse(new DefaultWarehouse(1, "w1"));
        warehouseManager.addWarehouse(new DefaultWarehouse(2, "w2"));

        WarehouseIdleChecker.increaseRunningSQL(1L);

        Config.warehouse_idle_check_enable = true;

        IdleStatus idleStatus = GlobalStateMgr.getCurrentState().getWarehouseIdleChecker().getIdleStatus();
        Assert.assertFalse(idleStatus.isClusterIdle);
        Assert.assertEquals(3, idleStatus.warehouses.size());
        for (int i = 0; i < idleStatus.warehouses.size(); i++) {
            IdleStatus.WarehouseStatus status = idleStatus.warehouses.get(i);
            if (status.id == 1L) {
                Assert.assertFalse(status.isIdle);
            } else {
                Assert.assertTrue(status.isIdle);
            }
        }
    }
}
