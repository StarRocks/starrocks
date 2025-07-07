// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.delta.DeltaLakeConnector;
import com.starrocks.connector.delta.DeltaLakeMetadata;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class DeltaUnityConnectorTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        NodeMgr nodeMgr = new NodeMgr();
        Frontend frontend = new Frontend(0, FrontendNodeType.LEADER, "", "", 0);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getMySelf();
                minTimes = 0;
                result = frontend;
            }
        };
    }

    @Test
    public void testCreateDatabricksConnector(@Mocked WorkspaceClient workspaceClient) {
        setUp();
        Map<String, String> properties = ImmutableMap.of("databricks.host", "https://xxxx.cloud.databricks.com",
                "type", "deltalake", "databricks.token", "xxxx",
                "databricks.catalog.name", "databricks_catalog",
                "hive.metastore.type", "unity");

        MockDatabricksWorkspaceClient.MockCatalogAPI mockCatalogAPI = new MockDatabricksWorkspaceClient.MockCatalogAPI(
                new MockDatabricksWorkspaceClient.MockCatalogsService());

        new Expectations(workspaceClient) {
            {
                workspaceClient.catalogs();
                result = mockCatalogAPI;
                minTimes = 0;
            }
        };

        DeltaLakeConnector deltaUnityConnector = new DeltaLakeConnector(new ConnectorContext("databricks0",
                "deltalake", properties));
        ConnectorMetadata metadata = deltaUnityConnector.getMetadata();
        Assert.assertTrue(metadata instanceof DeltaLakeMetadata);
        DeltaLakeMetadata deltaUnityMetadata = (DeltaLakeMetadata) metadata;
        Assert.assertEquals("databricks0", deltaUnityMetadata.getCatalogName());
        Assert.assertSame(deltaUnityMetadata.getMetastoreType(), MetastoreType.UNITY);
    }

    @Test
    public void testCreateDatabricksConnectorWithException1() {
        setUp();
        Map<String, String> properties = ImmutableMap.of("databricks.host", "https://xxxx.cloud.databricks.com",
                "type", "deltalake", "databricks.token", "xxxx",
                "hive.metastore.type", "unity");

        WorkspaceClient workspaceClient = new WorkspaceClient(new DatabricksConfig().
                setHost("https://xxxx.cloud.databricks.com").setToken("xxxx"));
        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("databricks0",
                "databricks_catalog", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Databricks catalog name must be set");
        DeltaLakeConnector deltaUnityConnector = new DeltaLakeConnector(new ConnectorContext("databricks0",
                "deltalake", properties));
    }

    @Test
    public void testCreateDatabricksConnectorWithException2() {
        setUp();
        Map<String, String> properties = ImmutableMap.of("databricks.host", "https://xxxx.cloud.databricks.com",
                "type", "deltalake",
                "databricks.catalog.name", "databricks_catalog",
                "hive.metastore.type", "unity");

        WorkspaceClient workspaceClient = new WorkspaceClient(new DatabricksConfig().
                setHost("https://xxxx.cloud.databricks.com").setToken("xxxx"));
        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("databricks0",
                "databricks_catalog", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Databricks Catalog need to set databricks.token or " +
                "databricks.client.id and databricks.client.secret");
        DeltaLakeConnector deltaUnityConnector = new DeltaLakeConnector(new ConnectorContext("databricks0",
                "deltalake", properties));
    }

    @Test
    public void testCreateDatabricksConnectorWithException3() {
        setUp();
        Map<String, String> properties = ImmutableMap.of("databricks.host", "https://xxxx.cloud.databricks.com",
                "type", "deltalake",
                "databricks.catalog.name", "databricks_catalog",
                "databricks.client.id", "aaa-bbb",
                "hive.metastore.type", "unity");

        WorkspaceClient workspaceClient = new WorkspaceClient(new DatabricksConfig().
                setHost("https://xxxx.cloud.databricks.com").setToken("xxxx"));
        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("databricks0",
                "databricks_catalog", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Databricks Catalog need to set databricks.token or " +
                "databricks.client.id and databricks.client.secret");
        DeltaLakeConnector deltaUnityConnector = new DeltaLakeConnector(new ConnectorContext("databricks0",
                "deltalake", properties));
    }
}