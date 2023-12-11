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

import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class BaseTableInfoTest {
    @Test
    public void testBaseTableInfo(@Mocked GlobalStateMgr globalStateMgr,
                                  @Mocked MetadataMgr metadataMgr,
                                  @Mocked CatalogMgr catalogManager,
                                  @Mocked ConnectorMetadata connectorMetadata,
                                  @Mocked Database database) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getCatalogMgr();
                result = catalogManager;

                catalogManager.catalogExists(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                result = true;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;

                metadataMgr.getOptionalMetadata(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                result = Optional.of(connectorMetadata);

                connectorMetadata.getDb(100L);
                result = database;

                connectorMetadata.getDb(200L);
                result = null;

                database.getTable(10L);
                result = null;
            }
        };
        BaseTableInfo baseTableInfo = new BaseTableInfo(100L, 10L);
        Assert.assertNull(baseTableInfo.getTable());
        Assert.assertNull(baseTableInfo.getTableByName());
        BaseTableInfo baseTableInfo2 = new BaseTableInfo(200L, 10L);
        Assert.assertNull(baseTableInfo2.getTable());
        Assert.assertNull(baseTableInfo2.getTableByName());
    }
}
