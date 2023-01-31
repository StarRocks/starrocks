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
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UseWarehouseStmtTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        String createWarehouse = "create warehouse aaa";
        starRocksAssert.withWarehouse(createWarehouse);
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
    }

    @Test
    public void testParserAndAnalyzer() {
        String sql = "use warehouse aaa";
        AnalyzeTestUtil.analyzeSuccess(sql);

        String sql_2 = "USE xxx aaa";
        AnalyzeTestUtil.analyzeFail(sql_2);
    }

    @Test
    public void testUse(@Mocked WarehouseManager warehouseMgr) throws Exception {
        new Expectations() {
            {
                warehouseMgr.warehouseExists("aaa");
                result = true;
                minTimes = 0;
            }
        };

        ctx.setQueryId(UUIDUtil.genUUID());
        StmtExecutor executor = new StmtExecutor(ctx, "use warehouse aaa");
        executor.execute();

        Assert.assertEquals("aaa", ctx.getCurrentWarehouse());

        executor = new StmtExecutor(ctx, "use xxx aaa");
        executor.execute();
        Assert.assertSame(ctx.getState().getStateType(), QueryState.MysqlStateType.ERR);
    }
}
