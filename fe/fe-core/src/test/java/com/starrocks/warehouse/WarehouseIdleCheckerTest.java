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
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseProperty;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WarehouseIdleCheckerTest {

    @Test
    public void getStatus() {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.initDefaultWarehouse();
        LocalWarehouse warehouse = new LocalWarehouse(1L, "test", new WarehouseProperty(), "");

        warehouseManager.addWarehouse(warehouse);

        WarehouseIdleChecker.increaseRunningSQL(1L);

        Config.warehouse_idle_check_enable = true;

        IdleStatus idleStatus = GlobalStateMgr.getCurrentState().getWarehouseIdleChecker().getIdleStatus(true);
        Assertions.assertFalse(idleStatus.isClusterIdle);
        Assertions.assertEquals(2, idleStatus.warehouses.size());
        for (int i = 0; i < idleStatus.warehouses.size(); i++) {
            IdleStatus.WarehouseStatus status = idleStatus.warehouses.get(i);
            if (status.id == 1L) {
                Assertions.assertFalse(status.isIdle);
            } else {
                Assertions.assertTrue(status.isIdle);
            }
        }
    }

    @Test
    public void testGetStatusResume() throws Exception {
        Config.warehouse_idle_check_enable = true;
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.initDefaultWarehouse();
        LocalWarehouse warehouse = new LocalWarehouse(1L, "test", new WarehouseProperty(), "");
        warehouseManager.addWarehouse(warehouse);

        long now = System.currentTimeMillis();
        IdleStatus idleStatus1 = GlobalStateMgr.getCurrentState().getWarehouseIdleChecker().getIdleStatus(true);
        Assertions.assertTrue(idleStatus1.isClusterIdle);
        IdleStatus.WarehouseStatus warehouseStatus1 = idleStatus1.warehouses.stream()
                .filter(w -> w.id == 1).findAny().get();
        Assertions.assertTrue(warehouseStatus1.isIdle);
        Assertions.assertTrue(warehouseStatus1.idleTime >= now);

        final long sleepTimeMs = 2000L;
        Thread.sleep(sleepTimeMs);
        warehouse.resumeSelf();

        IdleStatus idleStatus2 = GlobalStateMgr.getCurrentState().getWarehouseIdleChecker().getIdleStatus(true);
        Assertions.assertTrue(idleStatus2.isClusterIdle);
        IdleStatus.WarehouseStatus warehouseStatus2 = idleStatus2.warehouses.stream()
                .filter(w -> w.id == 1).findAny().get();
        Assertions.assertTrue(warehouseStatus2.isIdle);
        Assertions.assertTrue(warehouseStatus2.idleTime >= warehouseStatus1.idleTime + sleepTimeMs);
    }
}
