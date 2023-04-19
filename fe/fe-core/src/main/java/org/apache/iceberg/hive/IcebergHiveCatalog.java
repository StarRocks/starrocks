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

package org.apache.iceberg.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class IcebergHiveCatalog extends BaseMetastoreCatalog implements IcebergCatalog, Configurable<Configuration> {
    public static final String LOCATION_PROPERTY = "location";
    private static final Logger LOG = LogManager.getLogger(IcebergHiveCatalog.class);

    private String name;
    private Configuration conf;
    private FileIO fileIO;
    private ClientPool<IMetaStoreClient, TException> clients;

    @VisibleForTesting
    public IcebergHiveCatalog() {
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.HIVE_CATALOG;
    }

    @Override
    public Table loadTable(IcebergTable table) throws StarRocksConnectorException {
        TableIdentifier tableId = TableIdentifier.of(table.getRemoteDbName(), table.getRemoteTableName());
        return loadTable(tableId, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) throws StarRocksConnectorException {
        return loadTable(tableIdentifier, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier, Optional<IcebergMetricsReporter> metricsReporter) {
        return loadTable(tableIdentifier, null, null, metricsReporter);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> properties) throws StarRocksConnectorException {
        return loadTable(tableId, tableLocation, properties, Optional.empty());
    }

    private Table loadTable(TableIdentifier tableId, String tableLocation, Map<String, String> properties,
                            Optional<IcebergMetricsReporter> metricsReporter) throws StarRocksConnectorException {
        Preconditions.checkState(tableId != null);
        try {
            TableOperations ops = this.newTableOps(tableId);
            if (metricsReporter.isPresent()) {
                return new BaseTable(ops, fullTableName(this.name(), tableId), metricsReporter.get());
            }
            return new BaseTable(ops, fullTableName(this.name(), tableId));
        } catch (Exception e) {
            throw new StarRocksConnectorException(String.format(
                    "Failed to load Iceberg table with id: %s", tableId), e);
        }
    }

    @Override
    public void initialize(String inputName, Map<String, String> properties) {
        this.name = inputName;
        if (conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = new Configuration();
        }

        if (properties.containsKey(CatalogProperties.URI)) {
            this.conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get(CatalogProperties.URI));
        }

        if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
            this.conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    properties.get(CatalogProperties.WAREHOUSE_LOCATION));
        }

        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        this.fileIO =
                fileIOImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

        // warp cache fileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO(fileIO);
        cachingFileIO.initialize(properties);
        this.fileIO = cachingFileIO;

        this.clients = new CachedClientPool(conf, properties);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    @Override
    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new HiveTableOperations(conf, clients, fileIO, name, dbName, tableName);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> listAllDatabases() {
        try {
            return new ArrayList<>(clients.run(IMetaStoreClient::getAllDatabases));
        } catch (TException | InterruptedException e) {
            throw new RuntimeException(e);
        }
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

        dropDatabaseInHiveMetastore(dbName);
    }

    public void dropDatabaseInHiveMetastore(String dbName) {
        try {
            clients.run(
                    client -> {
                        client.dropDatabase(dbName, true, false);
                        return null;
                    });
        } catch (TException e) {
            LOG.error("Failed to drop database {}", dbName, e);
            throw new StarRocksConnectorException("Failed to drop database " + dbName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarRocksConnectorException("Interrupted in call to dropDatabase(name) " +
                    dbName + " in Hive Metastore. msg: %s", e.getMessage());
        }
    }

    @Override
    public Database getDB(String dbName) throws InterruptedException, TException {
        org.apache.hadoop.hive.metastore.api.Database db = clients.run(client -> client.getDatabase(dbName));
        if (db == null || db.getName() == null) {
            throw new TException("Hive db " + dbName + " doesn't exist");
        }
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName, db.getLocationUri());
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) {
        Database database = new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
        properties = properties == null ? new HashMap<>() : properties;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(LOCATION_PROPERTY)) {
                try {
                    URI uri = new Path(value).toUri();
                    FileSystem fileSystem = FileSystem.get(uri, new Configuration());
                    fileSystem.exists(new Path(value));
                } catch (Exception e) {
                    LOG.error("Invalid location URI: {}", value, e);
                    throw new StarRocksConnectorException("Invalid location URI: %s. msg: %s", value, e.getMessage());
                }
                database.setLocation(value);
            } else {
                throw new IllegalArgumentException("Unrecognized property: " + key);
            }
        }

        org.apache.hadoop.hive.metastore.api.Database hiveDb = HiveMetastoreApiConverter.toMetastoreApiDatabase(database);
        createHiveDatabase(hiveDb);
    }

    public void createHiveDatabase(org.apache.hadoop.hive.metastore.api.Database hiveDb) {
        try {
            clients.run(
                    client -> {
                        client.createDatabase(hiveDb);
                        return null;
                    });

            LOG.info("Created database: {}", hiveDb.getName());
        } catch (TException e) {
            LOG.error("Failed to create database {}", hiveDb.getName(), e);
            throw new StarRocksConnectorException("Failed to create database " + hiveDb.getName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarRocksConnectorException("Interrupted in call to createDatabase(name) " +
                    hiveDb + " in Hive Metastore. msg: %s", e.getMessage());
        }
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        String database = namespace.level(0);
        try {
            List<String> tableNames = clients.run(client -> client.getAllTables(database));
            return tableNames.stream().map(tblName -> TableIdentifier.of(namespace, tblName)).collect(Collectors.toList());
        } catch (TException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Transaction newCreateTableTransaction(
            String dbName,
            String tableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties) {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = newTableOps(TableIdentifier.of(Namespace.of(dbName), tableName));
        return createTableTransaction(tableName, ops, metadata);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!isValidIdentifier(identifier)) {
            return false;
        }

        String database = identifier.namespace().level(0);

        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata = null;
        if (purge) {
            try {
                lastMetadata = ops.current();
            } catch (NotFoundException e) {
                LOG.warn("Failed to load table metadata for table: {}, continuing drop without purge", identifier, e);
            }
        }

        try {
            clients.run(
                    client -> {
                        client.dropTable(
                                database,
                                identifier.name(),
                                false /* do not delete data */,
                                false /* throw NoSuchObjectException if the table doesn't exist */);
                        return null;
                    });

            if (purge && lastMetadata != null) {
                CatalogUtil.dropTableData(ops.io(), lastMetadata);
            }

            deleteTableDirectory(ops.current().location());
            LOG.info("Dropped table: {}", identifier);
            return true;
        } catch (NoSuchTableException | NoSuchObjectException e) {
            LOG.info("Skipping drop, table does not exist: {}", identifier, e);
            return false;
        } catch (TException e) {
            throw new StarRocksConnectorException("Failed to drop " + identifier, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarRocksConnectorException("Interrupted in call to dropTable", e);
        }
    }

    private void deleteTableDirectory(String tableLocation) {
        Path path = new Path(tableLocation);
        URI uri = path.toUri();
        try {
            FileSystem fileSystem = FileSystem.get(uri, new Configuration());
            fileSystem.delete(path, true);
        } catch (IOException e) {
            LOG.error("Failed to delete directory {}", tableLocation, e);
            throw new StarRocksConnectorException("Failed to delete directory %s. msg: %s", tableLocation, e.getMessage());
        }
    }

    @Override
    public String defaultTableLocation(String dbName, String tblName) {
        try {
            String location = clients.run(client -> client.getDatabase(dbName)).getLocationUri();
            if (Strings.isNullOrEmpty(location)) {
                throw new StarRocksConnectorException("Database %s location is not set", dbName);
            }
            location = location.endsWith("/") ? location : location + "/";
            return location + tblName;
        } catch (TException | InterruptedException e) {
            throw new StarRocksConnectorException("Failed to get default table location on %s.%s", dbName, tblName);
        }
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("uri", this.conf == null ? "" : this.conf.get(HiveConf.ConfVars.METASTOREURIS.varname))
                .toString();
    }
}
