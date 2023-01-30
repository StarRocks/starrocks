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
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarehouseTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
        // create warehouse
        String createWarehouse = "CREATE WAREHOUSE test";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withWarehouse(createWarehouse);
    }

    private static void addCluster(String sql) throws Exception {
        AlterWarehouseStmt addClusterStmt = (AlterWarehouseStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().alterWarehouse(addClusterStmt);
    }

    private static void removeCluster(String sql) throws Exception {
        AlterWarehouseStmt removeClusterStmt = (AlterWarehouseStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().alterWarehouse(removeClusterStmt);
    }

    private static void modifyWarehouseProperty(String sql) throws Exception {
        AlterWarehouseStmt modifyWarehousePropertyStmt = (AlterWarehouseStmt)
                UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().alterWarehouse(modifyWarehousePropertyStmt);
    }

    @Test
    public void testNormal(@Mocked StarOSAgent starOSAgent) throws DdlException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };

        new Expectations() {
            {
                starOSAgent.deleteWorkerGroup(anyLong);
                result = null;
                minTimes = 0;

                starOSAgent.createWorkerGroup(anyString);
                result = -1L;
                minTimes = 0;
            }
        };

        ExceptionChecker.expectThrowsNoException(
                () -> addCluster("alter warehouse test add cluster")
        );

        ExceptionChecker.expectThrowsNoException(
                () -> addCluster("alter warehouse test add cluster")
        );

        ExceptionChecker.expectThrowsNoException(
                () -> removeCluster("alter warehouse test remove cluster")
        );

        ExceptionChecker.expectThrowsNoException(
                () -> modifyWarehouseProperty("alter warehouse test  set(\"size\"=\"medium\");")
        );
        Assert.assertEquals("medium",
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse("test").getSize());

        ExceptionChecker.expectThrowsNoException(
                () -> modifyWarehouseProperty("alter warehouse test set(\"min_cluster\"=\"2\");")
        );
        Assert.assertEquals(2, GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse("test").getMinCluster());

        ExceptionChecker.expectThrowsNoException(
                () -> modifyWarehouseProperty("alter warehouse test set(\"max_cluster\"=\"4\");")
        );
        Assert.assertEquals(4, GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse("test").getMaxCluster());
    }
}
