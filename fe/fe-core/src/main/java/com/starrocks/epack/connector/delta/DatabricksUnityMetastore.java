// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.AwsCredentials;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.GcpOauthToken;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialRequest;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.starrocks.catalog.Database;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.gcp.GCPCloudConfigurationProvider;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class DatabricksUnityMetastore implements IMetastore {
    private static final Logger LOG = LogManager.getLogger(DatabricksUnityMetastore.class);
    public static final String DATABRICKS_HOST = "databricks.host";
    public static final String DATABRICKS_TOKEN = "databricks.token";
    public static final String DATABRICKS_CLIENT_ID = "databricks.client.id";
    public static final String DATABRICKS_CLIENT_SECRET = "databricks.client.secret";
    public static final String DATABRICKS_CATALOG_NAME = "databricks.catalog.name";
    public static final String DATABRICKS_VENDED_CREDENTIALS_ENABLED = "databricks.vended-credentials-enabled";

    private final String catalogName;
    private final String databricksCatalogName;

    private final WorkspaceClient workspaceClient;
    private final HdfsEnvironment hdfsEnvironment;
    private final DeltaLakeCatalogProperties deltaLakeCatalogProperties;
    private final boolean vendedCredentialsEnabled;

    public DatabricksUnityMetastore(String catalogName, String databricksCatalogName,
                                    WorkspaceClient workspaceClient,
                                    HdfsEnvironment hdfsEnvironment,
                                    DeltaLakeCatalogProperties properties) {
        this.catalogName = catalogName;
        this.databricksCatalogName = databricksCatalogName;
        this.workspaceClient = workspaceClient;
        this.hdfsEnvironment = hdfsEnvironment;
        this.deltaLakeCatalogProperties = properties;
        this.vendedCredentialsEnabled = PropertyUtil.propertyAsBoolean(properties.getProperties(),
                DATABRICKS_VENDED_CREDENTIALS_ENABLED, true);
    }

    @Override
    public List<String> getAllDatabaseNames() {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "UNITY.getAllDatabases")) {
            List<String> dbNames = Lists.newArrayList();
            try {
                dbNames = Streams.stream(workspaceClient.schemas().list(databricksCatalogName).iterator()).
                        map(SchemaInfo::getName).collect(Collectors.toList());
            } catch (NullPointerException e) {
                LOG.warn("Null pointer exception when get all databases from {} catalog", databricksCatalogName);
            } catch (Exception e) {
                LOG.error("Catalog {} get all databases failed", databricksCatalogName, e);
                throw e;
            }
            return dbNames;
        }
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "UNITY.getAllTables")) {
            List<String> tableNames = Lists.newArrayList();
            try {
                tableNames = Streams.stream(workspaceClient.tables().list(databricksCatalogName, dbName).iterator()).
                        filter(tableInfo -> tableInfo.getDataSourceFormat() == DataSourceFormat.DELTA).
                        map(TableInfo::getName).collect(Collectors.toList());
            } catch (NullPointerException e) {
                // empty database will throw null pointer exception, catch here and return empty list
                LOG.warn("Null pointer exception when get all tables from {}.{}", databricksCatalogName, dbName);
            } catch (Exception e) {
                LOG.error("Database {}.{} get all tables failed", databricksCatalogName, dbName, e);
                throw e;
            }
            return tableNames;
        }
    }

    @Override
    public Database getDb(String dbName) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "UNITY.getDatabase")) {
            SchemaInfo schemaInfo = workspaceClient.schemas().get(databricksCatalogName + "." + dbName);
            if (schemaInfo == null) {
                throw new StarRocksConnectorException("Databricks database [%s] doesn't exist", dbName);
            }
            return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(), schemaInfo.getName(),
                    schemaInfo.getStorageLocation());
        }
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "UNITY.getMetastoreTable")) {
            String fullName = Joiner.on(".").join(databricksCatalogName, dbName, tableName);
            TableInfo tableInfo = workspaceClient.tables().get(fullName);
            if (tableInfo == null) {
                return null;
            }
            if (tableInfo.getDataSourceFormat() != DataSourceFormat.DELTA) {
                return null;
            }
            CloudConfiguration cloudConfiguration = null;
            if (vendedCredentialsEnabled) {
                try {
                    cloudConfiguration = getVendedCredentials(tableInfo);
                } catch (Exception e) {
                    LOG.warn("Get vended credentials for table {} failed, use the default credentials. error: {}",
                            fullName, e.getMessage());
                }
            }
            String path = tableInfo.getStorageLocation();
            long createTime = tableInfo.getCreatedAt();
            return new MetastoreTable(dbName, tableName, path, createTime, cloudConfiguration);
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        String fullName = Joiner.on(".").join(databricksCatalogName, dbName, tblName);
        TableInfo tableInfo = workspaceClient.tables().get(fullName);
        return tableInfo != null;
    }

    public CloudConfiguration getVendedCredentials(TableInfo tableInfo) {
        CloudConfiguration cloudConfiguration = null;
        // try to get the temporary credentials
        GenerateTemporaryTableCredentialResponse response = workspaceClient.temporaryTableCredentials().
                generateTemporaryTableCredentials(new GenerateTemporaryTableCredentialRequest().
                        setTableId(tableInfo.getTableId()).setOperation(TableOperation.READ));

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (response.getAwsTempCredentials() != null) {
            AwsCredentials credentials = response.getAwsTempCredentials();
            builder.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, credentials.getAccessKeyId())
                    .put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, credentials.getSecretAccessKey())
                    .put(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, credentials.getSessionToken())
                    .put(CloudConfigurationConstants.AWS_S3_REGION,
                            deltaLakeCatalogProperties.getProperties().
                                    get(CloudConfigurationConstants.AWS_S3_REGION));
        } else if (response.getGcpOauthToken() != null) {
            // if the vended credentials is GCP, we can use the OAuth token directly
            GcpOauthToken gcpOauthToken = response.getGcpOauthToken();
            builder.put(GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN, gcpOauthToken.getOauthToken())
                    .put(GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN_EXPIRES_AT,
                            String.valueOf(response.getExpirationTime()));
        } else if (response.getAzureUserDelegationSas() != null) {
            // if the vended credentials is Azure, we can use the user delegation SAS token
            Path path = new Path(response.getUrl());
            String authority = path.toUri().getAuthority();
            if (authority == null || !authority.contains("@")) {
                throw new StarRocksConnectorException("Invalid Azure authority: %s", authority);
            }
            String endPoint = authority.split("@")[1];
            builder.put(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT, endPoint)
                    .put(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN, response.getAzureUserDelegationSas().getSasToken());
        }
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(builder.build());
        return cloudConfiguration;
    }
}