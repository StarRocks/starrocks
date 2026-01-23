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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class SetWarehouseStmtTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testParserAndAnalyzer() {
        AnalyzeTestUtil.setConnectContext(ctx);
        String sql = "set warehouse aaa";
        AnalyzeTestUtil.analyzeSuccess(sql);
    }

    @Test
    public void testSetWarehouse() {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.addWarehouse(new LocalWarehouse(12343L, "aaa", null, "mock warehouse for ut"));

        ctx.setQueryId(UUIDUtil.genUUID());
        Assertions.assertDoesNotThrow(() -> {
            StmtExecutor executor =
                    new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser("SET WAREHOUSE aaa", ctx));
            executor.execute();
        });
        Assertions.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());
        Assert.assertEquals("aaa", ctx.getCurrentWarehouseName());

        ctx.setQueryId(UUIDUtil.genUUID());
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("set xxx=aaa", ctx));
        Assert.assertTrue(exception.getMessage().contains("Unknown system variable 'xxx'"));
    }
}
