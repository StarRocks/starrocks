// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class DatabricksUnityMetastoreTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testGetAllDatabaseNamesWithException1(@Mocked WorkspaceClient workspaceClient) {
        new Expectations() {
            {
                workspaceClient.schemas();
                result = new NullPointerException();
                minTimes = 0;
            }
        };

        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("delta0",
                "databricks0", workspaceClient, null);
        List<String> dbs = databricksUnityMetastore.getAllDatabaseNames();
        Assert.assertEquals(0, dbs.size());
    }

    @Test
    public void testGetAllDatabaseNamesWithException2(@Mocked WorkspaceClient workspaceClient) {
        new Expectations() {
            {
                workspaceClient.schemas();
                result = new RuntimeException("unknown error");
                minTimes = 0;
            }
        };

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("unknown error");
        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("delta0",
                "databricks0", workspaceClient, null);
        databricksUnityMetastore.getAllDatabaseNames();
    }

    @Test
    public void testGetAllTableNamesWithException1(@Mocked WorkspaceClient workspaceClient) {
        new Expectations() {
            {
                workspaceClient.tables();
                result = new NullPointerException();
                minTimes = 0;
            }
        };

        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("delta0",
                "databricks0", workspaceClient, null);
        List<String> dbs = databricksUnityMetastore.getAllTableNames("db1");
        Assert.assertEquals(0, dbs.size());
    }

    @Test
    public void testGetAllTableNamesWithException2(@Mocked WorkspaceClient workspaceClient) {
        new Expectations() {
            {
                workspaceClient.tables();
                result = new RuntimeException("unknown error");
                minTimes = 0;
            }
        };

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("unknown error");
        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("delta0",
                "databricks0", workspaceClient, null);
        databricksUnityMetastore.getAllTableNames("db1");
    }

}
