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

import com.staros.proto.ShardInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class WarehouseTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
        starRocksAssert = new StarRocksAssert();
    }

    @Test
    public void testNormal() throws DdlException {
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assertions.assertTrue(warehouseMgr.warehouseExists(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
        Assertions.assertTrue(warehouseMgr.warehouseExists(WarehouseManager.DEFAULT_WAREHOUSE_ID));
    }

    @Test
    public void testGetComputeNodeAssignedToTablet(@Mocked ShardInfo shardInfo) {
        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getComputeNodeId(ComputeResource computeResource, long tabletId) {
                return null;
            }

            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return null;
            }
        };
        try {
            warehouseManager.getComputeNodeAssignedToTablet(WarehouseManager.DEFAULT_RESOURCE, 0);
            Assertions.fail();
        } catch (ErrorReportException e) {
            Assertions.assertTrue(e.getMessage().contains("No alive backend or compute node in warehouse"));
        }
    }

    @Test
    public void testGetWarehouse() {
        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
        Assertions.assertNotNull(warehouseManager.getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
        Assertions.assertNotNull(warehouseManager.getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
        Assertions.assertNull(warehouseManager.getWarehouseAllowNull("w"));
        Assertions.assertNull(warehouseManager.getWarehouseAllowNull(-1));
    }
}
