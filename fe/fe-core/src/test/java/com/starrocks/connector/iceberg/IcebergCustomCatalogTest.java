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


package com.starrocks.connector.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.hive.CachedClientPool;
import com.starrocks.connector.iceberg.hive.HiveTableOperations;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergUtil.convertToSRDatabase;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergCustomCatalog;

public class IcebergCustomCatalogTest {

    @Test
    public void testCatalogType(@Tested IcebergCustomTestingCatalog customTestingCatalog) {
        new MockUp<CatalogUtil>() {
            @Mock
            public Catalog loadCatalog(String catalogImpl, String catalogName,
                                       Map<String, String> properties,
                                       Configuration hadoopConf) {
                return customTestingCatalog;
            }
        };

        String catalogImpl = IcebergCustomTestingCatalog.class.getName();
        Map<String, String> icebergProperties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        IcebergCatalog customCatalog = IcebergUtil.getIcebergCustomCatalog(catalogImpl, icebergProperties, hdfsEnvironment);
        Assert.assertEquals(IcebergCatalogType.CUSTOM_CATALOG, customCatalog.getIcebergCatalogType());
    }

    @Test
    public void testLoadTable(@Mocked IcebergCustomTestingCatalog customTestingCatalog) {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("db", "table");
        new Expectations() {
            {
                customTestingCatalog.loadTable(identifier);
                result = new BaseTable(null, "test");
                minTimes = 0;
            }
        };

        new MockUp<CatalogUtil>() {
            @Mock
            public Catalog loadCatalog(String catalogImpl, String catalogName,
                                       Map<String, String> properties,
                                       Configuration hadoopConf) {
                return customTestingCatalog;
            }
        };

        String catalogImpl = IcebergCustomTestingCatalog.class.getName();
        Map<String, String> icebergProperties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        IcebergCatalog customCatalog = IcebergUtil.getIcebergCustomCatalog(catalogImpl, icebergProperties, hdfsEnvironment);
        Table table = customCatalog.loadTable(identifier);
        Assert.assertEquals("test", table.name());
    }

    @Test
    public void testListAllDatabases(@Mocked IcebergCustomTestingCatalog customCatalog) {
        new Expectations() {
            {
                customCatalog.listAllDatabases();
                result = Arrays.asList("db1", "db2");
                minTimes = 0;
            }
        };

        new MockUp<CatalogUtil>() {
            @Mock
            public Catalog loadCatalog(String catalogImpl, String catalogName,
                                       Map<String, String> properties,
                                       Configuration hadoopConf) {
                return customCatalog;
            }
        };

        String catalogImpl = IcebergCustomTestingCatalog.class.getName();
        Map<String, String> icebergProperties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        IcebergCustomTestingCatalog icebergCustomCatalog = (IcebergCustomTestingCatalog) getIcebergCustomCatalog(
                catalogImpl, icebergProperties, hdfsEnvironment);
        List<String> dbs = icebergCustomCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    public static class IcebergCustomTestingCatalog extends BaseMetastoreCatalog implements IcebergCatalog {

        private String name;
        private Configuration conf;
        private FileIO fileIO;
        private ClientPool<IMetaStoreClient, TException> clients;

        @VisibleForTesting
        public IcebergCustomTestingCatalog() {
        }

        @Override
        public IcebergCatalogType getIcebergCatalogType() {
            return IcebergCatalogType.CUSTOM_CATALOG;
        }

        @Override
        public Table loadTable(IcebergTable table) throws StarRocksIcebergException {
            TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(table);
            return loadTable(tableId, null, null);
        }

        @Override
        public Table loadTable(TableIdentifier tableIdentifier) throws StarRocksIcebergException {
            return loadTable(tableIdentifier, null, null);
        }

        @Override
        public Table loadTable(TableIdentifier tableId, String tableLocation,
                               Map<String, String> properties) throws StarRocksIcebergException {
            Preconditions.checkState(tableId != null);
            try {
                return super.loadTable(tableId);
            } catch (Exception e) {
                throw new StarRocksIcebergException(String.format(
                        "Failed to load Iceberg table with id: %s", tableId), e);
            }
        }

        @Override
        public void initialize(String inputName, Map<String, String> properties) {
            this.name = inputName;
            if (conf == null) {
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
        public Database getDB(String dbName) throws InterruptedException, TException {
            org.apache.hadoop.hive.metastore.api.Database db = clients.run(client -> client.getDatabase(dbName));
            if (db == null || db.getName() == null) {
                throw new TException("Hive db " + dbName + " doesn't exist");
            }
            return convertToSRDatabase(dbName);
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
        public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("uri", this.conf == null ? "" : this.conf.get(HiveConf.ConfVars.METASTOREURIS.varname))
                    .toString();
        }
    }
}
