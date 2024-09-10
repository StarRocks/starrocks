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


package com.starrocks.connector.iceberg.rest;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import com.starrocks.connector.share.iceberg.IcebergAwsClientFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMMENT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;

public class IcebergRESTCatalog implements IcebergCatalog {

    private static final Logger LOG = LogManager.getLogger(IcebergRESTCatalog.class);

    // Not all RestCatalog is tabular, here just used to handle tabular specifically
    // If we are using tabular rest catalog, we must use S3FileIO
    private static final String TABULAR_API = "https://api.tabular.io/ws";
    // This parameter we don't expose to user, just some people are using docker hosted tabular, it's url may
    // not the "https://api.tabular.io/ws"
    public static final String KEY_DISABLE_TABULAR_SUPPORT = "disable_tabular_support";
    public static final String KEY_CREDENTIAL_WITH_PREFIX = ICEBERG_CUSTOM_PROPERTIES_PREFIX + "credential";
    public static final String KEY_DISABLE_VENDED_CREDENTIAL = "disable_vended_credential";

    private final Configuration conf;
    private final RESTCatalog delegate;

    public IcebergRESTCatalog(String name, Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        Map<String, String> copiedProperties = Maps.newHashMap(properties);

        properties.forEach((key, value) -> {
            String newKey = key.toLowerCase();
            if (newKey.startsWith(ICEBERG_CUSTOM_PROPERTIES_PREFIX)) {
                newKey = newKey.substring(ICEBERG_CUSTOM_PROPERTIES_PREFIX.length());
                copiedProperties.put(newKey, value);
            }
        });

        copiedProperties.put(CatalogProperties.FILE_IO_IMPL, IcebergCachingFileIO.class.getName());
        copiedProperties.put(CatalogProperties.METRICS_REPORTER_IMPL, IcebergMetricsReporter.class.getName());

        if (!copiedProperties.containsKey(KEY_DISABLE_TABULAR_SUPPORT)) {
            copiedProperties.put("header.x-tabular-s3-access", "vended_credentials");
        }

        boolean disableVendedCredential = copiedProperties
                .getOrDefault(KEY_DISABLE_VENDED_CREDENTIAL, "false")
                .equalsIgnoreCase("true");
        if (disableVendedCredential) {
            copiedProperties.put(AwsProperties.CLIENT_FACTORY, IcebergAwsClientFactory.class.getName());
        }

        delegate = (RESTCatalog) CatalogUtil.loadCatalog(RESTCatalog.class.getName(), name, copiedProperties, conf);
    }

    // for ut
    public IcebergRESTCatalog(RESTCatalog restCatalog, Configuration conf) {
        this.delegate = restCatalog;
        this.conf = conf;
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.REST_CATALOG;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
        return delegate.loadTable(TableIdentifier.of(dbName, tableName));
    }

    @Override
    public boolean tableExists(String dbName, String tableName) throws StarRocksConnectorException {
        return delegate.tableExists(TableIdentifier.of(dbName, tableName));
    }

    @Override
    public List<String> listAllDatabases() {
        return delegate.listNamespaces().stream()
                .map(ns -> ns.level(0))
                .collect(Collectors.toList());
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) {
        properties = properties == null ? new HashMap<>() : properties;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(LOCATION_PROPERTY)) {
                try {
                    URI uri = new Path(value).toUri();
                    FileSystem fileSystem = FileSystem.get(uri, conf);
                    fileSystem.exists(new Path(value));
                } catch (Exception e) {
                    LOG.error("Invalid location URI: {}", value, e);
                    throw new StarRocksConnectorException("Invalid location URI: %s. msg: %s", value, e.getMessage());
                }
            } else {
                throw new IllegalArgumentException("Unrecognized property: " + key);
            }
        }
        Namespace ns = Namespace.of(dbName);
        delegate.createNamespace(ns, properties);
    }

    @Override
    public void dropDb(String dbName) throws MetaNotFoundException {
        Database database;
        try {
            database = getDB(dbName);
        } catch (Exception e) {
            LOG.error("Failed to access database {}", dbName, e);
            throw new MetaNotFoundException("Failed to access database " + dbName);
        }

        if (database == null) {
            throw new MetaNotFoundException("Not found database " + dbName);
        }

        String dbLocation = database.getLocation();
        if (Strings.isNullOrEmpty(dbLocation)) {
            throw new MetaNotFoundException("Database location is empty");
        }

        delegate.dropNamespace(Namespace.of(dbName));
    }

    @Override
    public Database getDB(String dbName) {
        Map<String, String> dbMeta = delegate.loadNamespaceMetadata(Namespace.of(dbName));
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName, dbMeta.get(LOCATION_PROPERTY));
    }

    @Override
    public List<String> listTables(String dbName) {
        List<TableIdentifier> tableIdentifiers = delegate.listTables(Namespace.of(dbName));
        List<TableIdentifier> viewIdentifiers = delegate.listViews(Namespace.of(dbName));
        if (!viewIdentifiers.isEmpty()) {
            tableIdentifiers.addAll(viewIdentifiers);
        }
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public boolean createTable(
            String dbName,
            String tableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties) {
        Table nativeTable = delegate.buildTable(TableIdentifier.of(dbName, tableName), schema)
                .withLocation(location)
                .withPartitionSpec(partitionSpec)
                .withProperties(properties)
                .create();

        return nativeTable != null;
    }

    @Override
    public boolean dropTable(String dbName, String tableName, boolean purge) {
        return delegate.dropTable(TableIdentifier.of(dbName, tableName), purge);
    }

    @Override
    public void renameTable(String dbName, String tblName, String newTblName) throws StarRocksConnectorException {
        delegate.renameTable(TableIdentifier.of(dbName, tblName), TableIdentifier.of(dbName, newTblName));
    }

    @Override
    public boolean createView(ConnectorViewDefinition definition, boolean replace) {
        Schema schema = IcebergApiConverter.toIcebergApiSchema(definition.getColumns());
        ViewBuilder viewBuilder = delegate.buildView(TableIdentifier.of(definition.getDatabaseName(), definition.getViewName()));
        viewBuilder = viewBuilder.withSchema(schema)
                .withQuery("starrocks", definition.getInlineViewDef())
                .withDefaultNamespace(Namespace.of(definition.getDatabaseName()))
                .withDefaultCatalog(definition.getCatalogName())
                .withProperties(buildProperties(definition))
                .withLocation(defaultTableLocation(definition.getDatabaseName(), definition.getViewName()));

        if (replace) {
            viewBuilder.createOrReplace();
        } else {
            viewBuilder.create();
        }

        return true;
    }

    public boolean dropView(String dbName, String viewName) {
        return delegate.dropView(TableIdentifier.of(dbName, viewName));
    }

    @Override
    public View getView(String dbName, String viewName) {
        return delegate.loadView(TableIdentifier.of(dbName, viewName));
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        if (fileLocations.isEmpty()) {
            return;
        }

        URI uri = new Path(fileLocations.get(0)).toUri();
        try {
            FileSystem fileSystem = FileSystem.get(uri, conf);
            for (String location : fileLocations) {
                Path path = new Path(location);
                fileSystem.delete(path, false);
            }
        } catch (Exception e) {
            LOG.error("Failed to delete uncommitted files", e);
        }
    }

    public String toString() {
        return delegate.toString();
    }

    @Override
    public String defaultTableLocation(String dbName, String tableName) {
        Map<String, String> properties = delegate.loadNamespaceMetadata(Namespace.of(dbName));
        String databaseLocation = properties.get(LOCATION_PROPERTY);
        checkArgument(databaseLocation != null, "location must be set for %s.%s", dbName, tableName);

        if (databaseLocation.endsWith("/")) {
            return databaseLocation + tableName;
        } else {
            return databaseLocation + "/" + tableName;
        }
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(String dbName) {
        return ImmutableMap.copyOf(delegate.loadNamespaceMetadata(Namespace.of(dbName)));
    }

    private Map<String, String> buildProperties(ConnectorViewDefinition definition) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            throw new StarRocksConnectorException("not found connect context when building iceberg view properties");
        }

        String queryId = connectContext.getQueryId().toString();

        Map<String, String> properties = ImmutableMap.of(
                "queryId", queryId,
                "starrocksCatalog", delegate.name(),
                "starrocksVersion", GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFeVersion());

        if (!Strings.isNullOrEmpty(definition.getComment())) {
            properties.put(COMMENT, definition.getComment());
        }

        return properties;
    }
}
