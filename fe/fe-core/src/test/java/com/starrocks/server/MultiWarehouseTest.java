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

package com.starrocks.server;

import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MultiWarehouseTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getWarehouseMgr().clearWarehouse();
    }

    @Test
    public void testManagerWarehouse() {
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        // create default warehouse
        CreateWarehouseStmt defaultWh = new CreateWarehouseStmt(false, WarehouseManager.DEFAULT_WAREHOUSE_NAME, null, null);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Warehouse '" + WarehouseManager.DEFAULT_WAREHOUSE_NAME + "' already exists",
                () -> warehouseMgr.createWarehouse(defaultWh));

        String warehouseName = "wh1";
        CreateWarehouseStmt stmt = new CreateWarehouseStmt(false, warehouseName, null, null);
        ExceptionChecker.expectThrowsNoException(() -> warehouseMgr.createWarehouse(stmt));
        Warehouse wh1 = warehouseMgr.getWarehouse(warehouseName);
        Assertions.assertEquals(warehouseName, wh1.getName());

        // create again
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse '" + warehouseName + "' already exists",
                () -> warehouseMgr.createWarehouse(stmt));

        // create with if not exists
        CreateWarehouseStmt stmtExists = new CreateWarehouseStmt(true, warehouseName, null, null);
        ExceptionChecker.expectThrowsNoException(() -> warehouseMgr.createWarehouse(stmtExists));


        // drop default warehouse
        DropWarehouseStmt dropDefault = new DropWarehouseStmt(false, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Cannot drop default warehouse: " + WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                () -> warehouseMgr.dropWarehouse(dropDefault));

        // drop warehouse
        DropWarehouseStmt dropStmt = new DropWarehouseStmt(false, warehouseName);
        ExceptionChecker.expectThrowsNoException(() -> warehouseMgr.dropWarehouse(dropStmt));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: " + warehouseName + " not exist.",
                () -> warehouseMgr.getWarehouse(warehouseName));

        // drop with if exists
        DropWarehouseStmt dropStmtExists = new DropWarehouseStmt(true, warehouseName);
        ExceptionChecker.expectThrowsNoException(() -> warehouseMgr.dropWarehouse(dropStmtExists));

    }

    @Test
    public void testWarehouseImage() throws Exception {
        String warehouseName = "wh1";
        String sql = String.format("CREATE WAREHOUSE IF NOT EXISTS %s;", warehouseName);
        connectContext.executeSql(sql);
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = warehouseMgr.getWarehouse(warehouseName);
        Assertions.assertNotNull(warehouse);

        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        warehouseMgr.save(initialImage.getImageWriter());

        warehouseMgr.clearWarehouse();
        warehouseMgr.load(initialImage.getMetaBlockReader());
        List<Warehouse> allWarehouses = warehouseMgr.getAllWarehouses();
        Assertions.assertEquals(2, allWarehouses.size());
        warehouse = warehouseMgr.getWarehouse(warehouseName);
        Assertions.assertNotNull(warehouse);
        Warehouse defaultWh = warehouseMgr.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        Assertions.assertNotNull(defaultWh);

    }
}
