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

import com.google.common.collect.Lists;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiWarehouseTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
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
}
