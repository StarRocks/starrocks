// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.external.MockedMetadataMgrForHive;
import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;

import java.util.Map;

public class HivePlanTestBase extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        mockHiveCatalog();
    }

    private static void mockHiveCatalog() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("hive", "hive0", "", properties);

        GlobalStateMgr gsmMgr = connectContext.getGlobalStateMgr();
        MockedMetadataMgrForHive metadataMgr = new MockedMetadataMgrForHive(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);
    }
}
