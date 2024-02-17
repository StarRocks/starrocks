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


package com.starrocks.catalog;

import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BaseTableInfoTest {
    @Test
    public void testBaseTableInfo(@Mocked GlobalStateMgr globalStateMgr,
                                  @Mocked CatalogMgr catalogManager,
                                  @Mocked Database database) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                catalogManager.isInternalCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                result = true;

                database.getTable(10L);
                result = null;
            }
        };

        BaseTableInfo baseTableInfo = new BaseTableInfo(100L, "db", "tbl1", 10L);
        Assert.assertTrue(baseTableInfo.getDb() != null);
        Assert.assertNull(baseTableInfo.getTable());
        BaseTableInfo baseTableInfo2 = new BaseTableInfo(200L, "db", "tbl2", 10L);
        Assert.assertTrue(baseTableInfo.getDb() != null);
        Assert.assertNull(baseTableInfo.getTable());
    }
}
