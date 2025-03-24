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

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.metadata.MetadataTableName;
import com.starrocks.connector.metadata.iceberg.LogicalIcebergMetadataTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.metadata.MetadataTableType.LOGICAL_ICEBERG_METADATA;

public class MetadataMgrTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createTbl = "create table db1.tbl1(k1 varchar(32), catalog varchar(32), external varchar(32), k4 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')";
        String createCatalog =
                "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        starRocksAssert.withTable(createTbl);
    }

    @Test
    public void testListDbNames(@Mocked HiveMetaStoreClient metaStoreThriftClient) throws TException, DdlException {
        new Expectations() {
            {
                metaStoreThriftClient.getAllDatabases();
                result = Lists.newArrayList("db2");
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<String> internalListDbs = metadataMgr.listDbNames(AnalyzeTestUtil.getConnectContext(), "default_catalog");
        Assert.assertTrue(internalListDbs.contains("db1"));

        List<String> externalListDbs = metadataMgr.listDbNames(AnalyzeTestUtil.getConnectContext(), "hive_catalog");
        Assert.assertTrue(externalListDbs.contains("db2"));
    }

    @Test
    public void testListTblNames(@Mocked HiveMetaStoreClient metaStoreThriftClient) throws TException, DdlException {
        new Expectations() {
            {
                metaStoreThriftClient.getAllTables("db2");
                result = Lists.newArrayList("tbl2");
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        List<String> internalTables = metadataMgr.listTableNames(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db1");
        Assert.assertTrue(internalTables.contains("tbl1"));
        try {
            metadataMgr.listTableNames(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db_foo");
            Assert.fail();
        } catch (StarRocksConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Database db_foo doesn't exist"));
        }

        List<String> externalTables = metadataMgr.listTableNames(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "db2");
        Assert.assertTrue(externalTables.contains("tbl2"));
        externalTables = metadataMgr.listTableNames(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "db3");
        Assert.assertTrue(externalTables.isEmpty());
    }

    @Test
    public void testGetDb(@Mocked HiveMetaStoreClient metaStoreThriftClient) throws TException {
        new Expectations() {
            {
                metaStoreThriftClient.getDatabase("db2");
                result = new Database("db2", "", "", null);
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        com.starrocks.catalog.Database database =
                metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db1");
        Assert.assertNotNull(database);
        database = metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db1");
        Assert.assertNotNull(database);

        com.starrocks.catalog.Database database1 = metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "db2");
        Assert.assertNotNull(database1);
        Assert.assertEquals("hive_catalog.db2", database1.getUUID());

        com.starrocks.catalog.Database database2 = metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "db3");
        Assert.assertNull(database2);

        Assert.assertNull(metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "not_exist_catalog", "xxx"));
    }

    @Test
    public void testGetTableWithDefaultCatalog() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Assert.assertTrue(metadataMgr.getOptionalMetadata("").isPresent());
        com.starrocks.catalog.Table internalTable =
                metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db1", "tbl1");
        Assert.assertEquals(internalTable.getName(), "tbl1");
    }

    @Test
    public void testGetTable(@Mocked HiveMetaStoreClient metaStoreThriftClient) throws TException {
        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "BIGINT", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hive";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        sd.setInputFormat(MAPRED_PARQUET_INPUT_FORMAT_CLASS);
        Table msTable1 = new Table();
        msTable1.setDbName("hive_db");
        msTable1.setTableName("hive_table");
        msTable1.setPartitionKeys(partKeys);
        msTable1.setSd(sd);
        msTable1.setTableType("MANAGED_TABLE");
        msTable1.setCreateTime(20201010);
        new Expectations() {
            {
                metaStoreThriftClient.getTable("hive_db", "hive_table");
                result = msTable1;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        com.starrocks.catalog.Table internalTable =
                metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db1", "tbl1");
        Assert.assertNotNull(internalTable);
        Assert.assertNull(metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "default_catalog", "not_exist_db", "xxx"));
        Assert.assertNull(metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "default_catalog", "db1", "not_exist_table"));

        com.starrocks.catalog.Table tbl1 =
                metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db", "hive_table");
        Assert.assertNotNull(tbl1);
        Assert.assertEquals("hive_catalog.hive_db.hive_table.20201010", tbl1.getUUID());

        com.starrocks.catalog.Table tbl2 =
                metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "not_exist_catalog", "xxx", "xxx");
        Assert.assertNull(tbl2);

        Assert.assertThrows(StarRocksConnectorException.class,
                () -> metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "not_exist_db", "xxx"));
        Assert.assertThrows(StarRocksConnectorException.class,
                () -> metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db", "not_exist_tbl"));
    }

    @Test
    public void testTableExists() throws TException {
        MetadataMgr metadataMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadataMgr) {
            {
                metadataMgr.tableExists(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db", "iceberg_tbl");
                result = true;
            }
        };
        Assert.assertTrue(
                metadataMgr.tableExists(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db", "iceberg_tbl"));
    }

    @Test
    public void testCreateIcebergTable() throws Exception {
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        AnalyzeTestUtil.getStarRocksAssert().withCatalog(createIcebergCatalogStmt);
        MetadataMgr metadataMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db");
                result = new com.starrocks.catalog.Database();
                minTimes = 0;

                metadataMgr.tableExists(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db", "iceberg_table");
                result = false;
            }
        };

        String stmt = "create external table iceberg_catalog.iceberg_db.iceberg_table (k1 int, k2 int) partition by (k2)";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db");
                result = null;
                minTimes = 0;
            }
        };

        try {
            metadataMgr.createTable(AnalyzeTestUtil.getConnectContext(), createTableStmt);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DdlException);
            Assert.assertTrue(e.getMessage().contains("Unknown database"));
        }

        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db");
                result = new com.starrocks.catalog.Database();
                minTimes = 0;

                metadataMgr.tableExists(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db", "iceberg_table");
                result = true;
                minTimes = 0;
            }
        };

        try {
            metadataMgr.createTable(AnalyzeTestUtil.getConnectContext(), createTableStmt);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DdlException);
            Assert.assertTrue(e.getMessage().contains("Table 'iceberg_table' already exists"));
        }

        createTableStmt.setIfNotExists();
        Assert.assertFalse(metadataMgr.createTable(AnalyzeTestUtil.getConnectContext(), createTableStmt));
        AnalyzeTestUtil.getStarRocksAssert().dropCatalog("iceberg_catalog");
    }

    @Test
    public void testHiveCreateTableLike() throws Exception {
        class MockedHiveMetadataMgr extends MockedMetadataMgr {

            public MockedHiveMetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
                super(localMetastore, connectorMgr);
            }

            @Override
            public com.starrocks.catalog.Database getDb(ConnectContext context, String catalogName, String dbName) {
                return new com.starrocks.catalog.Database(0, "hive_db", "s3://test-db/");
            }

            @Override
            public com.starrocks.catalog.Table getTable(ConnectContext context, String catalogName, String dbName,
                                                        String tblName) {
                List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "INT", ""));
                List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
                String hdfsPath = "hdfs://127.0.0.1:10000/hive";
                StorageDescriptor sd = new StorageDescriptor();
                sd.setInputFormat(MAPRED_PARQUET_INPUT_FORMAT_CLASS);
                sd.setCols(unPartKeys);
                sd.setLocation(hdfsPath);
                Table msTable = new Table();
                msTable.setPartitionKeys(partKeys);
                msTable.setSd(sd);
                msTable.setTableType("MANAGED_TABLE");
                msTable.setTableName("hive_tbl");
                msTable.setDbName("hive_db");
                int createTime = (int) System.currentTimeMillis();
                msTable.setCreateTime(createTime);

                return HiveMetastoreApiConverter.toHiveTable(msTable, "hive_catalog");
            }

            @Override
            public boolean tableExists(ConnectContext context, String catalogName, String dbName, String tblName) {
                return (catalogName.equals("hive_catalog") && dbName.equals("hive_db") && tblName.equals("hive_tbl")) ||
                        (catalogName.equals("hive_catalog") && dbName.equals("hive_db") && tblName.equals("hive_tbl_1"));
            }
        }

        ConnectContext connectContext = AnalyzeTestUtil.getConnectContext();
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        MockedHiveMetadataMgr mockedHiveMetadataMgr = new MockedHiveMetadataMgr(
                connectContext.getGlobalStateMgr().getLocalMetastore(),
                connectContext.getGlobalStateMgr().getConnectorMgr());

        // set to mockedHiveMetadataMgr to pass Analyzer check
        GlobalStateMgr.getCurrentState().setMetadataMgr(mockedHiveMetadataMgr);
        MockedHiveMetadata mockedHiveMetadata = new MockedHiveMetadata();
        mockedHiveMetadataMgr.registerMockedMetadata("hive_catalog", mockedHiveMetadata);

        String stmt = "create external table hive_catalog_1.hive_db.hive_tbl_1 like hive_catalog.hive_db.hive_tbl";
        CreateTableLikeStmt createTableLikeStmt =
                (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        try {
            mockedHiveMetadataMgr.createTableLike(connectContext, createTableLikeStmt);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DdlException);
            Assert.assertTrue(e.getMessage().contains("Invalid catalog hive_catalog_1"));
        }

        stmt = "create external table hive_catalog.hive_db.hive_tbl_1 like hive_catalog.hive_db.hive_table";
        createTableLikeStmt =
                (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        try {
            mockedHiveMetadataMgr.createTableLike(connectContext, createTableLikeStmt);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof DdlException);
            Assert.assertTrue(e.getMessage().contains("Table 'hive_tbl_1' already exists"));
        }

        stmt = "create external table hive_catalog.hive_db.hive_tbl_2 like hive_catalog.hive_db.hive_tbl";
        createTableLikeStmt =
                (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        try {
            mockedHiveMetadataMgr.createTableLike(connectContext, createTableLikeStmt);
        } catch (Exception e) {
            Assert.assertNull(e);
        }

        stmt = "create external table if not exists hive_catalog.hive_db.hive_tbl_1 like hive_catalog.hive_db.hive_table";
        createTableLikeStmt =
                (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        try {
            mockedHiveMetadataMgr.createTableLike(connectContext, createTableLikeStmt);
        } catch (Exception e) {
            Assert.assertNull(e);
        }

        stmt = "create external table if not exists hive_catalog.hive_db.hive_tbl_2 like hive_catalog.hive_db.hive_table";
        createTableLikeStmt =
                (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        try {
            mockedHiveMetadataMgr.createTableLike(connectContext, createTableLikeStmt);
        } catch (Exception e) {
            Assert.assertNull(e);
        }

        // set back to original metadataMrg
        GlobalStateMgr.getCurrentState().setMetadataMgr(metadataMgr);
    }

    @Test
    public void testGetOptionalMetadata() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Optional<ConnectorMetadata> metadata = metadataMgr.getOptionalMetadata("hive_catalog");
        Assert.assertTrue(metadata.isPresent());
        metadata = metadataMgr.getOptionalMetadata("hive_catalog_not_exist");
        Assert.assertFalse(metadata.isPresent());
    }

    @Test
    public void testRemoveCache() {
        MetadataMgr mgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        Map<UUID, ConnectorMetadata> queryIdSet = new HashMap<>();
        for (int i = 0; i < Config.catalog_metadata_cache_size; i++) {
            UUID queryId = UUIDUtil.genUUID();
            ConnectContext.get().setQueryId(queryId);
            Optional<ConnectorMetadata> metadata = mgr.getOptionalMetadata("hive_catalog");
            Assert.assertTrue(metadata.isPresent());
            queryIdSet.put(queryId, metadata.get());
        }

        // cache evicted
        UUID queryId = UUIDUtil.genUUID();
        ConnectContext.get().setQueryId(queryId);
        Optional<ConnectorMetadata> metadata = mgr.getOptionalMetadata("hive_catalog");
        Assert.assertTrue(metadata.isPresent());
        Assert.assertFalse(queryIdSet.containsValue(metadata.get()));
        mgr.removeQueryMetadata();
    }

    @Test(expected = MetaNotFoundException.class)
    public void testDropDbIfExists() throws DdlException, MetaNotFoundException {
        MetadataMgr metadataMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db");
                result = null;
                minTimes = 0;
            }
        };
        metadataMgr.dropDb(AnalyzeTestUtil.getConnectContext(), "hive_catalog", "hive_db", false);
    }

    @Test
    public void testGetMetadataTable() throws Exception {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            boolean tableExists(String dbName, String tableName) {
                return true;
            }
        };

        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        AnalyzeTestUtil.getStarRocksAssert().withCatalog(createIcebergCatalogStmt);
        MetadataMgr metadataMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        com.starrocks.catalog.Table table =
                metadataMgr.getTable(AnalyzeTestUtil.getConnectContext(), "iceberg_catalog", "iceberg_db",
                        "t1$logical_iceberg_metadata");
        Assert.assertTrue(table instanceof LogicalIcebergMetadataTable);
        LogicalIcebergMetadataTable metadataTable = (LogicalIcebergMetadataTable) table;
        Assert.assertEquals("iceberg_db", metadataTable.getOriginDb());
        Assert.assertEquals("t1", metadataTable.getOriginTable());
        Assert.assertEquals(LOGICAL_ICEBERG_METADATA, metadataTable.getMetadataTableType());
        Assert.assertTrue(metadataTable.isSupported());
        AnalyzeTestUtil.getStarRocksAssert().dropCatalog("iceberg_catalog");
    }

    @Test
    public void testMetadataTableName() {
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid metadata table name",
                () -> MetadataTableName.from("aaabbb&aaa"));

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid metadata table name",
                () -> MetadataTableName.from("table$unknown_type"));

        MetadataTableName metadataTableName = MetadataTableName.from("iceberg_table$logical_iceberg_metadata");
        Assert.assertEquals("iceberg_table", metadataTableName.getTableName());
        Assert.assertEquals(LOGICAL_ICEBERG_METADATA, metadataTableName.getTableType());
        Assert.assertEquals("iceberg_table$logical_iceberg_metadata", metadataTableName.getTableNameWithType());
        Assert.assertEquals("iceberg_table$logical_iceberg_metadata", metadataTableName.toString());

        Assert.assertFalse(MetadataTableName.isMetadataTable("aaaaaaa"));
        Assert.assertFalse(MetadataTableName.isMetadataTable("table$"));
        Assert.assertFalse(MetadataTableName.isMetadataTable("table$unknown_type"));
        Assert.assertTrue(MetadataTableName.isMetadataTable("table$logical_iceberg_metadata"));
    }
}
