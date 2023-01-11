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

import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClusterTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        String createWhStmtStr = "create warehouse test";
        CreateWarehouseStmt stmt = (CreateWarehouseStmt) UtFrameUtils.parseStmtWithNewParser(createWhStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().createWarehouse(stmt);
    }

    private static void addCluster(String sql) throws Exception {
        AlterWarehouseStmt addClusterStmt = (AlterWarehouseStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse("aaa").addCluster();
    }

    private static void removeCluster(String sql) throws Exception {
        AlterWarehouseStmt removeClusterStmt = (AlterWarehouseStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse("aaa").removeCluster();
    }

    private static void modifyWarehouseProperty(String sql) throws Exception {
        AlterWarehouseStmt modifyWarehousePropertyStmt = (AlterWarehouseStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse("aaa").modifyCluterSize();
    }

    @Test
    public void testNormal() throws DdlException {
        ExceptionChecker.expectThrowsNoException(
                () -> addCluster("alter warehouse aaa add cluster")
        );

        ExceptionChecker.expectThrowsNoException(
                () -> modifyWarehouseProperty("alter warehouse aaa  set(\"size\"=\"medium\");")
        );

        ExceptionChecker.expectThrowsNoException(
                () -> removeCluster("alter warehouse aaa remove cluster")
        );
    }
}
