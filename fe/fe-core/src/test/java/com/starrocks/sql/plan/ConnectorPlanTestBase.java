// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;

import java.util.Map;

public class ConnectorPlanTestBase extends PlanTestBase {
    private static MockedMetadataMgr metadataMgr = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        GlobalStateMgr gsmMgr = connectContext.getGlobalStateMgr();
        metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);

        mockHiveCatalog();
    }

    private static void mockHiveCatalog() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("hive", "hive0", "", properties);

        MockedHiveMetadata mockedHiveMetadata = new MockedHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, mockedHiveMetadata);
    }
}