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

<<<<<<< HEAD

package com.starrocks.warehouse;

import com.starrocks.common.DdlException;
=======
package com.starrocks.warehouse;

import com.staros.proto.ShardInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.lake.LakeTablet;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
<<<<<<< HEAD
=======
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarehouseTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;
<<<<<<< HEAD
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
        starRocksAssert = new StarRocksAssert();
    }

    @Test
    public void testNormal() throws DdlException {
<<<<<<< HEAD
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
=======
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
        Assert.assertTrue(warehouseMgr.warehouseExists(WarehouseManager.DEFAULT_WAREHOUSE_ID));
    }

    @Test
    public void testGetComputeNodeAssignedToTablet(@Mocked ShardInfo shardInfo) {
        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
                return null;
            }
        };
        try {
            warehouseManager.getComputeNodeAssignedToTablet(0L, new LakeTablet(0));
            Assert.fail();
        } catch (ErrorReportException e) {
            Assert.assertTrue(e.getMessage().contains("No alive backend or compute node in warehouse"));
        }
    }

    @Test
    public void testGetWarehouse() {
        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
        Assert.assertNotNull(warehouseManager.getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
        Assert.assertNotNull(warehouseManager.getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
        Assert.assertNull(warehouseManager.getWarehouseAllowNull("w"));
        Assert.assertNull(warehouseManager.getWarehouseAllowNull(-1));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
}
