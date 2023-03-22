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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.hive.HiveTableOperations;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.CONNECTOR_ID_GENERATOR;

public class IcebergGlueCatalog extends BaseMetastoreCatalog implements IcebergCatalog, Configurable<Configuration> {
    private static final Logger LOG = LogManager.getLogger(IcebergGlueCatalog.class);

    private String name;
    private Configuration conf;
    private FileIO fileIO;
    private ClientPool<IMetaStoreClient, TException> clients;

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return IcebergCatalogType.GLUE_CATALOG;
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
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> properties) throws StarRocksConnectorException {
        Preconditions.checkState(tableId != null);
        try {
            TableOperations ops = this.newTableOps(tableId);
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

        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        this.fileIO =
                fileIOImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

        // warp cache fileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO(fileIO);
        cachingFileIO.initialize(properties);
        this.fileIO = cachingFileIO;

        properties.forEach(conf::set);
        this.clients = new CachedGlueClientPool(this.name, conf, properties);
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
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Database getDB(String dbName) throws InterruptedException, TException {
        org.apache.hadoop.hive.metastore.api.Database db = clients.run(client -> client.getDatabase(dbName));
        if (db == null || db.getName() == null) {
            throw new TException("Glue db " + dbName + " doesn't exist");
        }

        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        String database = namespace.level(0);
        try {
            List<String> tableNames = clients.run(client -> client.getAllTables(database));
            return tableNames.stream().map(tblName -> TableIdentifier.of(namespace, tblName)).collect(Collectors.toList());
        } catch (TException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
