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


package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;

import java.util.Map;

public class ConnectorPlanTestBase extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        GlobalStateMgr gsmMgr = connectContext.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);

        mockHiveCatalogImpl(metadataMgr);
        mockJDBCCatalogImpl(metadataMgr);
    }

    public static void mockCatalog(ConnectContext ctx) throws DdlException {
        GlobalStateMgr gsmMgr = ctx.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);
        mockHiveCatalogImpl(metadataMgr);
        mockJDBCCatalogImpl(metadataMgr);
        mockIcebergCatalogImpl(metadataMgr);
    }

    public static void mockHiveCatalog(ConnectContext ctx) throws DdlException {
        GlobalStateMgr gsmMgr = ctx.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);
        mockHiveCatalogImpl(metadataMgr);
    }

    private static void mockHiveCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("hive", "hive0", "", properties);

        MockedHiveMetadata mockedHiveMetadata = new MockedHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, mockedHiveMetadata);
    }

    public static void createPaimonTable(Catalog catalog, String db) throws Exception {
        catalog.createDatabase(db, false);

        // create partitioned table
        createParitionedTable(catalog, db);

        // create partitioned table
        createUnPartitionedTable(catalog, db);
    }

    private static void createUnPartitionedTable(Catalog catalog, String db) throws Exception {
        Identifier identifier = Identifier.create(db, "unpartitioned_table");
        Schema schema = new Schema(
                Lists.newArrayList(
                        new DataField(0, "pk", DataTypes.STRING(), "field1"),
                        new DataField(1, "d", DataTypes.STRING(), "field2")),
                Collections.emptyList(),
                Lists.newArrayList("pk"),
                org.apache.paimon.shade.guava30.com.google.common.collect.Maps.newHashMap(),
                "");
        catalog.createTable(
                identifier,
                schema,
                false);
        // create table
        org.apache.paimon.table.Table table = catalog.getTable(identifier);
        BatchTableWrite batchTableWrite = table.newBatchWriteBuilder().newWrite();
        BatchTableCommit batchTableCommit = table.newBatchWriteBuilder().newCommit();
        for (int i = 0; i < 10; i++) {
            GenericRow genericRow = new GenericRow(3);
            genericRow.setField(0, BinaryString.fromString(String.valueOf(i)));
            genericRow.setField(1, BinaryString.fromString("2"));
            batchTableWrite.write(genericRow);
        }
        batchTableCommit.commit(batchTableWrite.prepareCommit());
    }

    private static void createParitionedTable(Catalog catalog, String db) throws Exception {
        Identifier identifier = Identifier.create(db, "partitioned_table");
        Schema schema = new Schema(
                Lists.newArrayList(
                        new DataField(0, "pk", DataTypes.STRING(), "field1"),
                        new DataField(1, "d", DataTypes.STRING(), "field2"),
                        new DataField(2, "pt", DataTypes.DATE(), "field3")),
                Lists.newArrayList("pt"),
                Lists.newArrayList("pk", "pt"),
                org.apache.paimon.shade.guava30.com.google.common.collect.Maps.newHashMap(),
                "");
        catalog.createTable(
                identifier,
                schema,
                false);
        // create table
        org.apache.paimon.table.Table table = catalog.getTable(identifier);
        BatchTableWrite batchTableWrite = table.newBatchWriteBuilder().newWrite();
        BatchTableCommit batchTableCommit = table.newBatchWriteBuilder().newCommit();
        for (int i = 0; i < 10; i++) {
            GenericRow genericRow = new GenericRow(3);
            genericRow.setField(0, BinaryString.fromString("1"));
            genericRow.setField(1, BinaryString.fromString("2"));
            genericRow.setField(2, (int) LocalDate.now().toEpochDay() + i);
            batchTableWrite.write(genericRow);
        }
        batchTableCommit.commit(batchTableWrite.prepareCommit());
    }

    private static Catalog createPaimonCatalog(String warehouse) {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);
        return catalog;
    }

    private static void mockPaimonCatalogImpl(MockedMetadataMgr metadataMgr, String warehouse) throws Exception {
        //create paimon native table and write data
        Catalog paimonNativeCatalog = createPaimonCatalog(warehouse);
        createPaimonTable(paimonNativeCatalog, "pmn_db1");

        // create extern paimon catalog
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "paimon");
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("paimon.catalog.warehouse", warehouse);
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("paimon", "paimon0", "", properties);

        Options options = new Options();
        options.set(CatalogOptions.METASTORE, "filesystem");
        options.set(CatalogOptions.WAREHOUSE, warehouse);
        //register paimon catalog
        PaimonMetadata paimonMetadata =
                new PaimonMetadata("paimon0", new HdfsEnvironment(), paimonNativeCatalog, options);
        metadataMgr.registerMockedMetadata("paimon0", paimonMetadata);
    }

    public static class MockedDeltaLakeMetadata extends DeltaLakeMetadata {

        private static final String MOCKED_CATALOG_NAME = "deltalake_catalog";
        private static final String MOCKED_DB_NAME = "deltalake_db";
        private static final String MOCKED_TABLE_NAME = "tbl";

        private static final Map<String, DeltaLakeTable>
                MOCK_TABLE_MAP = new CaseInsensitiveMap<>();

        public MockedDeltaLakeMetadata() {
            super(null, null, null);

            long tableId = GlobalStateMgr.getCurrentState().getNextId();
            List<Column> columns = ImmutableList.<Column>builder()
                    .add(new Column("col1", Type.INT))
                    .add(new Column("col2", Type.STRING))
                    .build();
            List<String> partitionNames = new ArrayList<>();
            DeltaLog deltaLog = null;
            long createTime = System.currentTimeMillis();
            DeltaLakeTable table = new DeltaLakeTable(tableId, MOCKED_CATALOG_NAME, MOCKED_DB_NAME, MOCKED_TABLE_NAME,
                    columns, partitionNames, deltaLog, createTime);

            MOCK_TABLE_MAP.put(MOCKED_TABLE_NAME, table);
        }

        @Override
        public com.starrocks.catalog.Table getTable(String dbName, String tblName) {
            return MOCK_TABLE_MAP.get(tblName);
        }

        @Override
        public Database getDb(String dbName) {
            return new Database(GlobalStateMgr.getCurrentState().getNextId(), dbName);
        }
    }

    private static void mockDeltaLakeCatalog(MockedMetadataMgr metadataMgr) throws Exception {
        final String catalogName = MockedDeltaLakeMetadata.MOCKED_CATALOG_NAME;
        final String dbName = MockedDeltaLakeMetadata.MOCKED_DB_NAME;
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();

        // create catalog
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", Table.TableType.DELTALAKE.name())
                .put("hive.metastore.uris", "thrift://127.0.0.1:9083")
                .build();
        catalogMgr.createCatalog(Table.TableType.DELTALAKE.name(), catalogName, "", properties);

        //register catalog
        DeltaLakeMetadata metadata = new MockedDeltaLakeMetadata();
        metadataMgr.registerMockedMetadata(catalogName, metadata);
    }

    private static void mockJDBCCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put(JDBCResource.TYPE, "jdbc");
        properties.put(JDBCResource.DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mysql://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
        GlobalStateMgr.getCurrentState().getCatalogMgr().
                createCatalog("jdbc", MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, "", properties);

        MockedJDBCMetadata mockedJDBCMetadata = new MockedJDBCMetadata(properties);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, mockedJDBCMetadata);

        Map<String, String> pgProperties = ImmutableMap.<String, String>builder()
                .put(JDBCResource.TYPE, "jdbc")
                .put(JDBCResource.DRIVER_CLASS, "com.postgres.Driver")
                .put(JDBCResource.URI, "jdbc:postgres://127.0.0.1:3306")
                .put(JDBCResource.USER, "root")
                .put(JDBCResource.PASSWORD, "123456")
                .put(JDBCResource.CHECK_SUM, "xxxx")
                .put(JDBCResource.DRIVER_URL, "xxxx")
                .build();
        GlobalStateMgr.getCurrentState().getCatalogMgr().
                createCatalog("jdbc", MockedJDBCMetadata.MOCKED_JDBC_PG_CATALOG_NAME, "", pgProperties);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_PG_CATALOG_NAME,
                new MockedJDBCMetadata(pgProperties));
    }

    private static void mockIcebergCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("iceberg", "iceberg0", "", properties);

        MockIcebergMetadata mockIcebergMetadata = new MockIcebergMetadata();
        metadataMgr.registerMockedMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME, mockIcebergMetadata);
    }
}