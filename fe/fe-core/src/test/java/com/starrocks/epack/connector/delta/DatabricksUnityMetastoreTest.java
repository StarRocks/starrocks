// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.AwsCredentials;
import com.databricks.sdk.service.catalog.GcpOauthToken;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialRequest;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import io.trino.hive.$internal.com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
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
                "databricks0", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
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
                "databricks0", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
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
                "databricks0", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
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
                "databricks0", workspaceClient, null,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
        databricksUnityMetastore.getAllTableNames("db1");
    }

    @Test
    public void testVendedCredentialsEnabled(@Mocked WorkspaceClient workspaceClient,
                                             @Mocked TableInfo tableInfo,
                                             @Mocked GenerateTemporaryTableCredentialResponse response,
                                             @Mocked AwsCredentials awsCredentials) {
        HashMap<String, String> props = new HashMap<>();
        props.put(DatabricksUnityMetastore.DATABRICKS_VENDED_CREDENTIALS_ENABLED, "true");
        props.put(CloudConfigurationConstants.AWS_S3_REGION, "us-west-2");
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(props);
        // Mock TableInfo
        new Expectations() {
            {
                workspaceClient.tables().get(anyString);
                result = tableInfo;
                tableInfo.getDataSourceFormat();
                result = com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
                tableInfo.getTableId();
                result = "tableId";
                workspaceClient.temporaryTableCredentials().
                        generateTemporaryTableCredentials((GenerateTemporaryTableCredentialRequest) any);
                result = response;
                response.getAwsTempCredentials();
                result = awsCredentials;
                awsCredentials.getAccessKeyId();
                result = "ak";
                awsCredentials.getSecretAccessKey();
                result = "sk";
                awsCredentials.getSessionToken();
                result = "token";
                tableInfo.getStorageLocation();
                result = "s3://bucket/table";
                tableInfo.getCreatedAt();
                result = 123L;
            }
        };
        DatabricksUnityMetastore metastore = new DatabricksUnityMetastore("delta0", "databricks0",
                workspaceClient, null, properties);
        MetastoreTable table = metastore.getMetastoreTable("db", "tbl");
        Assert.assertNotNull(table);
        CloudConfiguration conf = table.getCloudConfiguration();
        Assert.assertNotNull(conf);
    }

    @Test
    public void testVendedCredentialsNotEnabled(@Mocked WorkspaceClient workspaceClient,
                                                @Mocked TableInfo tableInfo) {
        HashMap<String, String> props = new HashMap<>();
        props.put(DatabricksUnityMetastore.DATABRICKS_VENDED_CREDENTIALS_ENABLED, "false");
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(props);
        new Expectations() {
            {
                workspaceClient.tables().get(anyString);
                result = tableInfo;
                tableInfo.getDataSourceFormat();
                result = com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
                tableInfo.getStorageLocation();
                result = "s3://bucket/table";
                tableInfo.getCreatedAt();
                result = 123L;
            }
        };
        DatabricksUnityMetastore metastore = new DatabricksUnityMetastore("delta0", "databricks0",
                workspaceClient, null, properties);
        MetastoreTable table = metastore.getMetastoreTable("db", "tbl");
        Assert.assertNotNull(table);
        Assert.assertNull(table.getCloudConfiguration());
    }

    @Test
    public void testGCPVendingCredentials(@Mocked WorkspaceClient workspaceClient,
                                          @Mocked TableInfo tableInfo,
                                          @Mocked GenerateTemporaryTableCredentialResponse response,
                                          @Mocked GcpOauthToken gcpOauthToken) {
        HashMap<String, String> props = new HashMap<>();
        props.put(DatabricksUnityMetastore.DATABRICKS_VENDED_CREDENTIALS_ENABLED, "true");
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(props);
        // Mock TableInfo
        new Expectations() {
            {
                workspaceClient.tables().get(anyString);
                result = tableInfo;
                tableInfo.getDataSourceFormat();
                result = com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
                tableInfo.getTableId();
                result = "tableId";
                workspaceClient.temporaryTableCredentials().
                        generateTemporaryTableCredentials((GenerateTemporaryTableCredentialRequest) any);
                result = response;
                response.getAwsTempCredentials();
                result = null;
                response.getGcpOauthToken();
                result = gcpOauthToken;
                gcpOauthToken.getOauthToken();
                result = "access_token";
                response.getExpirationTime();
                result = 1234567890L;
                tableInfo.getStorageLocation();
                result = "gs://bucket/table";
                tableInfo.getCreatedAt();
                result = 123L;
            }
        };
        DatabricksUnityMetastore metastore = new DatabricksUnityMetastore("delta0", "databricks0",
                workspaceClient, null, properties);
        MetastoreTable table = metastore.getMetastoreTable("db", "tbl");
        Assert.assertNotNull(table);
        CloudConfiguration conf = table.getCloudConfiguration();
        Assert.assertNotNull(conf);
        Assert.assertEquals("GCPCloudConfiguration{resources='', jars='', hdpuser='', " +
                "cred=GCPCloudCredential{endpoint='', useComputeEngineServiceAccount=false, serviceAccountEmail='', " +
                "serviceAccountPrivateKeyId='', serviceAccountPrivateKey='', impersonationServiceAccount='', " +
                "accessToken='access_token', accessTokenExpiresAt='1234567890'}}", conf.toConfString());
    }
}
