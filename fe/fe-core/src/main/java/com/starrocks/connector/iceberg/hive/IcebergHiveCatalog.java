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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolationException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import com.starrocks.connector.share.iceberg.IcebergAwsClientFactory;
import com.starrocks.qe.ConnectContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
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

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergApiConverter.convertDbNameToNamespace;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_TIMEOUT;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;

public class IcebergHiveCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(IcebergHiveCatalog.class);

    private final Configuration conf;
    private final HiveCatalog delegate;
    private final String catalogName;

    @VisibleForTesting
    public IcebergHiveCatalog(String name, Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        this.catalogName = name;
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
        // The property is false by default, in such case, when we execute SHOW TABLES FROM CATALOG.DB,
        // it will request all Table Objects from Hive Metastore, when there are lots of tables under the
        // database, timeout may happen.
        copiedProperties.putIfAbsent(HiveCatalog.LIST_ALL_TABLES, "true");
        // Iceberg defaults the hms client pool to 2 connections, which caps the concurrency of every metadata
        // load on this metastore no matter how many threads are loading. Only fill in the fe-side default when
        // the catalog does not carry the property itself.
        copiedProperties.putIfAbsent(CatalogProperties.CLIENT_POOL_SIZE,
                String.valueOf(Config.iceberg_hms_client_pool_size));
        String clientPoolSize = copiedProperties.get(CatalogProperties.CLIENT_POOL_SIZE);
        String clientPoolSizeSource = properties.containsKey(CatalogProperties.CLIENT_POOL_SIZE)
                ? "catalog property " + CatalogProperties.CLIENT_POOL_SIZE
                : "fe config iceberg_hms_client_pool_size";

        delegate = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), name, copiedProperties, conf);
        LOG.info("Created iceberg hive catalog {}: metastore={}, hms client pool size={}, set by {}. "
                        + "Iceberg caches one hms client pool per metastore uri for the whole fe process, so this "
                        + "size is the real concurrency limit only when this is the first catalog created for that "
                        + "uri; a later catalog on the same uri reuses the earlier pool and its own size is ignored.",
                name, metastoreURI, clientPoolSize, clientPoolSizeSource);
    }

    @VisibleForTesting
    public IcebergHiveCatalog(HiveCatalog hiveCatalog, Configuration conf) {
        this.delegate = hiveCatalog;
        this.conf = conf;
        this.catalogName = hiveCatalog == null ? null : hiveCatalog.name();
    }

    /**
     * One request to the Hive metastore is about to be made; report it if this thread holds an FE
     * metadata lock.
     *
     * <p><b>Why this class needs its own door even though the FE has a guarded HMS client.</b> It does
     * not use it: iceberg's {@code HiveCatalog} keeps its own {@code ClientPool<IMetaStoreClient, ...>},
     * so nothing here passes {@code HiveMetaClient}. Assuming otherwise left every load, list, drop,
     * register and view request in this class unreported while a lock was held.
     *
     * <p>Kept as a private one-liner per catalog for the reason spelled out in {@code
     * IcebergGlueCatalog}: the report names the first frame outside the class that called the guard, so
     * a shared helper would make it point at this class instead of the caller that decided to go remote.
     * It carries the guard's own name so one grep finds every door.
     */
    private void validateNotUnderLock() {
        BlockingCallValidator.validateNotUnderLock(getIcebergCatalogType().transportTag(), catalogName);
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.HIVE_CATALOG;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tableName) throws StarRocksConnectorException {
        validateNotUnderLock();
        return delegate.loadTable(TableIdentifier.of(dbName, tableName));
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tableName) throws StarRocksConnectorException {
        validateNotUnderLock();
        return delegate.tableExists(TableIdentifier.of(dbName, tableName));
    }

    @Override
    public List<String> listAllDatabases(ConnectContext context) {
        validateNotUnderLock();
        return delegate.listNamespaces().stream()
                .map(ns -> ns.level(0))
                .collect(Collectors.toList());
    }

    @Override
    public void createDB(ConnectContext context, String dbName, Map<String, String> properties) {
        properties = properties == null ? new HashMap<>() : properties;
        // The door sits inside the location branch and before the try: a property map with nothing but
        // an unrecognized key waits on nothing and must not be reported, and in error mode the catch
        // below would rewrite the refusal into "Invalid location URI". Everything else about this loop is
        // as it was -- which value wins and which exception a mixed map raises must not change here.
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(LOCATION_PROPERTY)) {
                BlockingCallValidator.validateNotUnderLock("remote-storage");
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
        validateNotUnderLock();
        delegate.createNamespace(ns, properties);
    }

    @Override
    public void dropDB(ConnectContext context, String dbName) throws MetaNotFoundException {
        Database database;
        try {
            database = getDB(context, dbName);
        } catch (Exception e) {
            // getDB has a door, and in error mode it refuses rather than returns; rewritten as a
            // connector error the refusal would read as an unreachable catalog.
            LockInvariantViolationException.rethrowIfRefusal(e);
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

        validateNotUnderLock();
        delegate.dropNamespace(Namespace.of(dbName));
    }

    @Override
    public Database getDB(ConnectContext context, String dbName) {
        validateNotUnderLock();
        Map<String, String> dbMeta = delegate.loadNamespaceMetadata(Namespace.of(dbName));
        Preconditions.checkNotNull(dbMeta.get(LOCATION_PROPERTY), "Database " + dbName + " doesn't exist location");
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName, dbMeta.get(LOCATION_PROPERTY));
    }

    @Override
    public List<String> listTables(ConnectContext context, String dbName) {
        validateNotUnderLock();
        List<TableIdentifier> tableIdentifiers = delegate.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public boolean createTable(
            ConnectContext context,
            String dbName,
            String tableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            SortOrder sortOrder,
            Map<String, String> properties) {
        validateNotUnderLock();
        Table nativeTable =  delegate.buildTable(TableIdentifier.of(dbName, tableName), schema)
                .withLocation(location)
                .withPartitionSpec(partitionSpec)
                .withSortOrder(sortOrder)
                .withProperties(properties)
                .create();

        return nativeTable != null;
    }

    @Override
    public boolean dropTable(ConnectContext context, String dbName, String tableName, boolean purge) {
        validateNotUnderLock();
        return delegate.dropTable(TableIdentifier.of(dbName, tableName), purge);
    }

    @Override
    public void renameTable(ConnectContext context, String dbName, String tblName, String newTblName)
            throws StarRocksConnectorException {
        validateNotUnderLock();
        delegate.renameTable(TableIdentifier.of(dbName, tblName), TableIdentifier.of(dbName, newTblName));
    }

    @Override
    public ViewBuilder getViewBuilder(ConnectContext context, TableIdentifier identifier) {
        validateNotUnderLock();
        return delegate.buildView(identifier);
    }

    @Override
    public boolean dropView(ConnectContext context, String dbName, String viewName) {
        validateNotUnderLock();
        return delegate.dropView(TableIdentifier.of(convertDbNameToNamespace(dbName), viewName));
    }

    @Override
    public View getView(ConnectContext context, String dbName, String viewName) {
        validateNotUnderLock();
        return delegate.loadView(TableIdentifier.of(convertDbNameToNamespace(dbName), viewName));
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(ConnectContext context, Namespace ns) {
        validateNotUnderLock();
        return ImmutableMap.copyOf(delegate.loadNamespaceMetadata(ns));
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        if (fileLocations.isEmpty()) {
            return;
        }

        // Storage, not the catalog: deleting the files a failed commit left behind.
        BlockingCallValidator.validateNotUnderLock("remote-storage");
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

    @Override
    public boolean registerTable(ConnectContext context, String dbName, String tableName, 
                                 String metadataFileLocation) {
        // Outside the try: its catch turns everything into "Failed to register table", which in error
        // mode would hide the refusal.
        validateNotUnderLock();
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);
            Table table = delegate.registerTable(tableIdentifier, metadataFileLocation);
            return table != null;
        } catch (Exception e) {
            LOG.error("Failed to register table {}.{} with metadata file location {}", 
                    dbName, tableName, metadataFileLocation, e);
            throw new StarRocksConnectorException("Failed to register table: " + e.getMessage(), e);
        }
    }

    public String toString() {
        return delegate.toString();
    }
}
