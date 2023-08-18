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


package com.starrocks.analysis;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.epack.server.WarehouseManagerEpack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class SetWarehouseStmtTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(new HashSet<>(Collections.singletonList(-1L)));
    }

    @Test
    public void testParserAndAnalyzer() {
        String sql = "set warehouse aaa";
        AnalyzeTestUtil.analyzeSuccess(sql);
    }

    @Test
    public void testSetWarehouse(@Mocked WarehouseManagerEpack warehouseMgr) throws Exception {

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                warehouseMgr.warehouseExists("aaa");
                result = true;
                minTimes = 0;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new LocalWarehouse(12343L, "aaa", 11L, "no comments");
            }
        };

        ctx.setQueryId(UUIDUtil.genUUID());
        StmtExecutor executor = new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("SET WAREHOUSE aaa"), ctx));
        executor.execute();

        Assert.assertEquals("aaa", ctx.getCurrentWarehouseName());

        executor = new StmtExecutor(ctx, "set xxx=aaa");
        executor.execute();
        Assert.assertSame(ctx.getState().getStateType(), QueryState.MysqlStateType.ERR);
    }
}
