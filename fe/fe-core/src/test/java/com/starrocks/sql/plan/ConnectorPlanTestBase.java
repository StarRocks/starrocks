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

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;

import java.util.Map;

public class ConnectorPlanTestBase extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        GlobalStateMgr gsmMgr = connectContext.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);

        mockHiveCatalogImpl(metadataMgr);
        mockJDBCCatalogImpl(metadataMgr);
    }

    public static void mockCatalog(ConnectContext ctx) throws DdlException {
        GlobalStateMgr gsmMgr = ctx.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);
        mockHiveCatalogImpl(metadataMgr);
        mockJDBCCatalogImpl(metadataMgr);
        mockIcebergCatalogImpl(metadataMgr);
    }

    public static void mockHiveCatalog(ConnectContext ctx) throws DdlException {
        GlobalStateMgr gsmMgr = ctx.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);
        mockHiveCatalogImpl(metadataMgr);
    }

    private static void mockHiveCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("hive", "hive0", "", properties);

        MockedHiveMetadata mockedHiveMetadata = new MockedHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, mockedHiveMetadata);
    }

    private static void mockJDBCCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put(JDBCResource.TYPE, "jdbc");
        properties.put(JDBCResource.DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mysql://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
        GlobalStateMgr.getCurrentState().getCatalogMgr().
                createCatalog("jdbc", MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, "", properties);

        MockedJDBCMetadata mockedJDBCMetadata = new MockedJDBCMetadata(properties);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, mockedJDBCMetadata);

        Map<String, String> pgProperties = ImmutableMap.<String, String>builder()
                .put(JDBCResource.TYPE, "jdbc")
                .put(JDBCResource.DRIVER_CLASS, "com.postgres.Driver")
                .put(JDBCResource.URI, "jdbc:postgres://127.0.0.1:3306")
                .put(JDBCResource.USER, "root")
                .put(JDBCResource.PASSWORD, "123456")
                .put(JDBCResource.CHECK_SUM, "xxxx")
                .put(JDBCResource.DRIVER_URL, "xxxx")
                .build();
        GlobalStateMgr.getCurrentState().getCatalogMgr().
                createCatalog("jdbc", MockedJDBCMetadata.MOCKED_JDBC_PG_CATALOG_NAME, "", pgProperties);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_PG_CATALOG_NAME,
                new MockedJDBCMetadata(pgProperties));
    }

    private static void mockIcebergCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("iceberg", "iceberg0", "", properties);

        MockIcebergMetadata mockIcebergMetadata = new MockIcebergMetadata();
        metadataMgr.registerMockedMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME, mockIcebergMetadata);
    }
}