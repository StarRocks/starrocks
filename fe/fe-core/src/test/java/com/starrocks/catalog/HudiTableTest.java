// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.external.ColumnTypeConverter;
import com.starrocks.external.hive.HiveMetaClient;
import com.starrocks.external.hive.HiveMetastoreTest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TableFactory;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.avro.Schema;
import org.apache.hudi.exception.HoodieIOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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

        Assert.assertEquals("db0", oTable.getDbName());
        Assert.assertEquals("table0", oTable.getTableName());
        Assert.assertEquals(new Column("col1", Type.INT, true), oTable.getColumn("col1"));
        Assert.assertEquals("table0:" + createTime, oTable.getTableIdentifier());
        Assert.assertTrue(oTable.toString().contains("HudiTable{resourceName='catalog', catalogName='catalog', " +
                "hiveDbName='db0', hiveTableName='table0', id=2, name='table0', type=HUDI"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getTable(anyString, anyString, anyString);
                result = oTable;
            }
        };

        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"hudi0\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = TableFactory.createTable(createTableStmt, com.starrocks.catalog.Table.TableType.HUDI);
        Assert.fail("No exception throws.");
    }



    @Test(expected = DdlException.class)
    public void testNoDb() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"hudi0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = TableFactory.createTable(createTableStmt, com.starrocks.catalog.Table.TableType.HUDI);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"hudi0\", \"database\"=\"db0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = TableFactory.createTable(createTableStmt, com.starrocks.catalog.Table.TableType.HUDI);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoResource() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = TableFactory.createTable(createTableStmt, com.starrocks.catalog.Table.TableType.HUDI);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNotExistResource() throws Exception {
        String createTableSql = "create external table db.hudi_tbl (col1 int, col2 int) engine=hudi properties " +
                "(\"resource\"=\"not_exist\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = TableFactory.createTable(createTableStmt, com.starrocks.catalog.Table.TableType.HUDI);
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
                ScalarType.createDefaultString());
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                Schema.createArray(Schema.create(Schema.Type.INT))),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                Schema.createFixed("FIXED", "FIXED", "F", 1)),
                ScalarType.createType(PrimitiveType.VARCHAR));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                Schema.createMap(Schema.create(Schema.Type.INT))),
                ScalarType.createType(PrimitiveType.UNKNOWN_TYPE));
        Assert.assertEquals(ColumnTypeConverter.fromHudiType(
                Schema.createUnion(Schema.create(Schema.Type.INT))),
                ScalarType.createType(PrimitiveType.INT));
    }
}
