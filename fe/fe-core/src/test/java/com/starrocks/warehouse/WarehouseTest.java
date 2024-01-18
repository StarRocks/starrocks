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

import com.starrocks.cloudnative.warehouse.WarehouseManager;
import com.starrocks.common.exception.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
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
        starRocksAssert = new StarRocksAssert();
    }

    @Test
    public void testNormal() throws DdlException {
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
    }
}
