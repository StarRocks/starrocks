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


package com.starrocks.connector.iceberg.glue;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;

public class IcebergGlueCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(IcebergGlueCatalog.class);

    private final Configuration conf;
    private final GlueCatalog delegate;
    private final String catalogName;

    public IcebergGlueCatalog(String name, Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        this.catalogName = name;
        Map<String, String> copiedProperties = Maps.newHashMap(properties);

        copiedProperties.put(CatalogProperties.FILE_IO_IMPL, IcebergCachingFileIO.class.getName());
        copiedProperties.put(CatalogProperties.METRICS_REPORTER_IMPL, IcebergMetricsReporter.class.getName());
        copiedProperties.put(AwsProperties.CLIENT_FACTORY, IcebergAwsClientFactory.class.getName());
        copiedProperties.put(AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION, "true");
        // No door on construction, unlike the jdbc catalog (which opens its connection here) and the REST
        // one (which fetches /v1/config). Building the Glue client goes remote only when the credential
        // provider has to look something up -- an instance profile queries IMDS, static credentials with
        // an explicit region contact nothing -- and this guard reports a wait that is happening, never one
        // that might. Every actual Glue request is covered per method below.
        delegate = (GlueCatalog) CatalogUtil.loadCatalog(GlueCatalog.class.getName(), name, copiedProperties, conf);
    }

    /**
     * One request to the system this catalog wraps is about to be made; report it if this thread
     * holds an FE metadata lock. The third-party catalog has no FE-owned wrapper to guard, so the
     * guard goes on the methods that call it -- the same shape as {@code KuduMetadata}.
     *
     * <p><b>Why each catalog keeps its own copy of this one-liner instead of sharing one.</b> The
     * report names the first frame belonging to a class other than the one that called the guard, so
     * a private method here makes it name whoever called <em>this catalog</em> -- the caching layer
     * or the metadata facade, i.e. the code that decided to go remote. Moving the body to a shared
     * class or an interface default method would make every report point at this class's own method
     * instead, which is never the code that has to change. The tag itself is shared, on
     * {@link IcebergCatalogType#transportTag()}.
     *
     * <p>It carries the guard's own name on purpose: the point of these doors is that they can be
     * enumerated, and one grep for {@code validateNotUnderLock} has to find all of them.
     */
    private void validateNotUnderLock() {
        BlockingCallValidator.validateNotUnderLock(getIcebergCatalogType().transportTag(), catalogName);
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.GLUE_CATALOG;
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
                    throw new StarRocksConnectorException(
                            String.format("Invalid location URI: %s", value), e);
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
            throw new StarRocksConnectorException("Failed to access database " + dbName, e);
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
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName, dbMeta.getOrDefault(LOCATION_PROPERTY, ""));
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
        if (Strings.isNullOrEmpty(location)) {
            String dbLocation = getDB(context, dbName).getLocation();
            if (Strings.isNullOrEmpty(dbLocation)) {
                throw new StarRocksConnectorException("Failed to find location in database '%s'. Please define the location" +
                        " when you create table or recreate another database with location." +
                        " You could execute the SQL command like 'CREATE TABLE <table_name> <columns> " +
                        "PROPERTIES('location' = '<location>')", dbName);
            }
        }

        validateNotUnderLock();
        Table nativeTable = delegate.buildTable(TableIdentifier.of(dbName, tableName), schema)
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
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        if (fileLocations.isEmpty()) {
            return;
        }

        // Storage, not the catalog: deleting the files a failed commit left behind.
        BlockingCallValidator.validateNotUnderLock("remote-storage", catalogName);
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
        // mode would hide the refusal and why it was raised.
        validateNotUnderLock();
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);
            Table table = delegate.registerTable(tableIdentifier, metadataFileLocation);
            return table != null;
        } catch (Exception e) {
            LOG.error("Failed to register table {}.{} with metadata file location {}", 
                    dbName, tableName, metadataFileLocation, e);
            throw new StarRocksConnectorException(
                    String.format("Failed to register table %s.%s", dbName, tableName), e);
        }
    }

    public String toString() {
        return delegate.toString();
    }
}
