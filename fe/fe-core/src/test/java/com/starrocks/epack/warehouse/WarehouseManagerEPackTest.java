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

package com.starrocks.epack.warehouse;

import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class WarehouseManagerEPackTest {

    @Mocked
    private EditLog editLog;

    @Before
    public void before() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
    }

    @Test
    public void testWarehouseException() {
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        Map<Long, Warehouse> idToWh = Deencapsulation.getField(mgr, "idToWh");
        Map<String, Warehouse> nameToWh = Deencapsulation.getField(mgr, "nameToWh");
        LocalWarehouse wh1 = new LocalWarehouse(1L, "wh1", 1L, null, "");
        wh1.suspendSelf();
        idToWh.put(wh1.getId(), wh1);
        nameToWh.put(wh1.getName(), wh1);

        // warehouse suspend
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: wh1 has been suspended.",
                () -> mgr.getAllComputeNodeIds("wh1"));

        SuspendWarehouseStmt suspendStmt = new SuspendWarehouseStmt("wh1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh1 has been suspended.",
                () -> mgr.suspendWarehouse(suspendStmt));

        // warehouse exist
        CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh1", Maps.newHashMap(), "");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh1 already exists.",
                () -> mgr.createWarehouse(createStmt));

        // warehouse not exist
        DropWarehouseStmt dropStmt = new DropWarehouseStmt(false, "wh2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh2 not exist.",
                () -> mgr.dropWarehouse(dropStmt));
    }

    @Test
    public void testAlterWarehouse() throws DdlException {
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public void updateWorkerGroup(long workerGroupId, int replicaNumber) throws DdlException {
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };
        Map<String, String> m = new HashMap<>();
        m.put("compute_replica", "3");
        AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        mgr.initDefaultWarehouse();
        mgr.alterWarehouse(alterStmt);
    }
}
