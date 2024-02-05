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

package com.starrocks.connector.iceberg.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergAwsClientFactory;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_TIMEOUT;
import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_METASTORE_URIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;

public class IcebergHiveCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(IcebergHiveCatalog.class);
    public static final String LOCATION_PROPERTY = "location";

    private final Configuration conf;
    private final HiveCatalog delegate;

    @VisibleForTesting
    public IcebergHiveCatalog(String name, Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        String hmsTimeout = properties.getOrDefault(HIVE_METASTORE_TIMEOUT, String.valueOf(Config.hive_meta_store_timeout_s));
        this.conf.set(MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT.getHiveName(), hmsTimeout);
        if (conf.get(METASTOREWAREHOUSE.varname) == null) {
            this.conf.set(METASTOREWAREHOUSE.varname, METASTOREWAREHOUSE.getDefaultValue());
        }

        Map<String, String> copiedProperties = Maps.newHashMap(properties);

        String metastoreURI = properties.get(HIVE_METASTORE_URIS);
        if (metastoreURI == null) {
            metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
        }
        Util.validateMetastoreUris(metastoreURI);

        copiedProperties.put(CatalogProperties.URI, metastoreURI);
        copiedProperties.put(CatalogProperties.FILE_IO_IMPL, IcebergCachingFileIO.class.getName());
        copiedProperties.put(AwsProperties.CLIENT_FACTORY, IcebergAwsClientFactory.class.getName());
        copiedProperties.put(CatalogProperties.METRICS_REPORTER_IMPL, IcebergMetricsReporter.class.getName());

        delegate = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), name, copiedProperties, conf);
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.HIVE_CATALOG;
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
        Preconditions.checkNotNull(dbMeta.get(LOCATION_PROPERTY), "Database " + dbName + " doesn't exist location");
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName, dbMeta.get(LOCATION_PROPERTY));
    }

    @Override
    public List<String> listTables(String dbName) {
        List<TableIdentifier> tableIdentifiers = delegate.listTables(Namespace.of(dbName));
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
        Table nativeTable =  delegate.buildTable(TableIdentifier.of(dbName, tableName), schema)
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
}
