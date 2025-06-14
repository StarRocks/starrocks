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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.HudiTableFactory;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.avro.Schema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieIOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.server.ExternalTableFactory.RESOURCE;

public class HudiTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private HiveMetaClient hiveClient;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withResource("create external resource 'hudi0' PROPERTIES(" +
                "\"type\"  =  \"hudi\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")");
        starRocksAssert.withDatabase("db");
        hiveClient = new HiveMetastoreTest.MockedHiveMetaClient();
    }

    com.starrocks.catalog.Table createTable(CreateTableStmt stmt) throws DdlException {
        return TableFactoryProvider.getFactory(EngineType.HUDI.name()).createTable(null, null, stmt);
    }

    @Test(expected = HoodieIOException.class)
    public void testCreateExternalTable(@Mocked MetadataMgr metadataMgr) throws Exception {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("col1", Type.INT, true));
        columns.add(new Column("col2", Type.INT, true));
        columns.add(new Column("_hoodie_commit_time", Type.STRING, true));
        columns.add(new Column("_hoodie_commit_seqno", Type.STRING, true));
        columns.add(new Column("_hoodie_record_key", Type.STRING, true));
        columns.add(new Column("_hoodie_partition_path", Type.STRING, true));
        columns.add(new Column("_hoodie_file_name", Type.STRING, true));
        long createTime = System.currentTimeMillis();
        List<String> dataColumns = Lists.newArrayList("col2", "_hoodie_commit_time", "_hoodie_commit_seqno",
                "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name");

        Map<String, String> properties = Maps.newHashMap();
        properties.put("hudi.table.base.path", "hdfs://127.0.0.1:10000/hudi");
        HudiTable.Builder tableBuilder = HudiTable.builder()
                .setId(2)
                .setTableName("table0")
                .setCatalogName("catalog")
                .setHiveDbName("db0")
                .setHiveTableName("table0")
                .setResourceName("catalog")
                .setFullSchema(columns)
                .setDataColNames(dataColumns)
                .setPartitionColNames(Lists.newArrayList("col1"))
                .setCreateTime(createTime)
                .setHudiProperties(properties);
        HudiTable oTable = tableBuilder.build();

        Assert.assertEquals("db0", oTable.getCatalogDBName());
        Assert.assertEquals("table0", oTable.getCatalogTableName());
        Assert.assertEquals(new Column("col1", Type.INT, true), oTable.getColumn("col1"));
        Assert.assertEquals("table0:" + createTime, oTable.getTableIdentifier());
        Assert.assertTrue(oTable.toString().contains("HudiTable{resourceName='catalog', catalogName='catalog', " +
                "hiveDbName='db0', hiveTableName='table0', id=2, name='table0', type=HUDI"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getTable((ConnectContext) any, anyString, anyString, anyString);
                result = oTable;
            }
        };

        String createTableSql = "create external table if not exists db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"hudi0\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoDb() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"hudi0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"hudi0\", \"database\"=\"db0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoResource() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNotExistResource() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"not_exist\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testInputFormat() {
        Assert.assertEquals(HudiTable.HudiTableType.COW,
                HudiTable.fromInputFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat"));
        Assert.assertEquals(HudiTable.HudiTableType.COW,
                HudiTable.fromInputFormat("com.uber.hoodie.hadoop.HoodieInputFormat"));
        Assert.assertEquals(HudiTable.HudiTableType.MOR,
                HudiTable.fromInputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat"));
        Assert.assertEquals(HudiTable.HudiTableType.MOR,
                HudiTable.fromInputFormat("com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat"));
        Assert.assertEquals(HudiTable.HudiTableType.UNKNOWN,
                HudiTable.fromInputFormat("org.apache.hadoop.hive.ql.io.HiveInputFormat"));
    }

    @Test
    public void testColumnTypeConvert() {
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(Schema.create(Schema.Type.BOOLEAN)),
                ScalarType.createType(PrimitiveType.BOOLEAN));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(Schema.create(Schema.Type.INT)),
                ScalarType.createType(PrimitiveType.INT));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(Schema.create(Schema.Type.FLOAT)),
                ScalarType.createType(PrimitiveType.FLOAT));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(Schema.create(Schema.Type.DOUBLE)),
                ScalarType.createType(PrimitiveType.DOUBLE));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(Schema.create(Schema.Type.STRING)),
                ScalarType.createDefaultCatalogString());
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                        Schema.createArray(Schema.create(Schema.Type.INT))),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                        Schema.createFixed("FIXED", "FIXED", "F", 1)),
                ScalarType.createType(PrimitiveType.VARCHAR));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                        Schema.createMap(Schema.create(Schema.Type.INT))),
                new MapType(ScalarType.createDefaultCatalogString(), ScalarType.createType(PrimitiveType.INT)));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                        Schema.createUnion(Schema.create(Schema.Type.INT))),
                ScalarType.createType(PrimitiveType.INT));
    }

    @Test
    public void testToThrift(
            @Mocked ConnectorMgr connectorMgr,
            @Mocked CatalogConnector catalogConnector,
            @Mocked ConnectorMetadata connectorMetadata,
            @Mocked HoodieTableMetaClient hoodieTableMetaClient) {
        new Expectations() {
            {
                connectorMgr.getConnector(anyString);
                result = catalogConnector;
            }

            {
                catalogConnector.getMetadata();
                result = connectorMetadata;
            }

            {
                connectorMetadata.getCloudConfiguration();
                result = new CloudConfiguration();
                times = 1;
            }
        };

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("col1", Type.INT, true));
        columns.add(new Column("col2", Type.INT, true));
        long createTime = System.currentTimeMillis();

        Map<String, String> properties = Maps.newHashMap();
        properties.put("hudi.table.base.path", "hdfs://127.0.0.1:10000/hudi");
        HudiTable.Builder tableBuilder = HudiTable.builder()
                .setId(2)
                .setTableName("table0")
                .setCatalogName("catalog")
                .setHiveDbName("db0")
                .setHiveTableName("table0")
                .setResourceName("catalog")
                .setFullSchema(columns)
                .setPartitionColNames(Lists.newArrayList("col1"))
                .setCreateTime(createTime)
                .setHudiProperties(properties);
        HudiTable table = tableBuilder.build();

        TTableDescriptor tTableDescriptor = table.toThrift(ImmutableList.of());
        Assert.assertEquals("db0", tTableDescriptor.getDbName());
        Assert.assertEquals("table0", tTableDescriptor.getTableName());
    }

    @Test
    public void testCreateTableResourceName() throws DdlException {
        String resourceName = "Hudi_resource_29bb53dc_7e04_11ee_9b35_00163e0e489a";
        Map<String, String> properties = new HashMap() {
            {
                put(RESOURCE, resourceName);
            }
        };
        HudiTable.Builder tableBuilder = HudiTable.builder()
                .setId(1000)
                .setTableName("supplier")
                .setCatalogName("hudi_catalog")
                .setHiveDbName("hudi_oss_tpch_1g_parquet_gzip")
                .setHiveTableName("supplier")
                .setResourceName(resourceName)
                .setFullSchema(new ArrayList<>())
                .setDataColNames(new ArrayList<>())
                .setPartitionColNames(Lists.newArrayList())
                .setCreateTime(10)
                .setHudiProperties(new HashMap<>());
        HudiTable oTable = tableBuilder.build();

        HudiTable.Builder newBuilder = HudiTable.builder();
        HudiTableFactory.copyFromCatalogTable(newBuilder, oTable, properties);
        HudiTable table = newBuilder.build();
        Assert.assertEquals(table.getResourceName(), resourceName);
    }
}
