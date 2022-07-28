//// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
//
//package com.starrocks.connector.hive;
//
//import com.google.common.collect.Lists;
//import com.starrocks.catalog.Column;
//import com.starrocks.catalog.HiveTable;
//import com.starrocks.catalog.Type;
//import com.starrocks.common.DdlException;
//import com.starrocks.external.hive.HiveMetaStoreThriftClient;
//import mockit.Expectations;
//import mockit.Mocked;
//import org.apache.hadoop.hive.metastore.api.FieldSchema;
//import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
//import org.apache.hadoop.hive.metastore.api.Table;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.List;
//
//public class HiveMetadataTest {
//    @Test
//    public void testListDatabaseNames(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws Exception {
//        new Expectations() {
//            {
//                metaStoreThriftClient.getAllDatabases();
//                result = Lists.newArrayList("db1", "db2");
//                minTimes = 0;
//            }
//        };
//
//        String metastoreUris = "thrift://127.0.0.1:9083";
//        HiveMetadata metadata = new HiveMetadata(metastoreUris);
//        List<String> expectResult = Lists.newArrayList("db1", "db2");
//        Assert.assertEquals(expectResult, metadata.listDbNames());
//    }
//
//    @Test
//    public void testListTableNames(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws Exception {
//        String db1 = "db1";
//        String db2 = "db2";
//
//        new Expectations() {
//            {
//                metaStoreThriftClient.getAllTables(db1);
//                result = Lists.newArrayList("tbl1", "tbl2");
//                minTimes = 0;
//            }
//        };
//
//        String metastoreUris = "thrift://127.0.0.1:9083";
//        HiveMetadata metadata = new HiveMetadata(metastoreUris);
//        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
//        Assert.assertEquals(expectResult, metadata.listTableNames(db1));
//    }
//
//    @Test
//    public void testListTableNamesOnNotExistDb() throws Exception {
//        String db2 = "db2";
//        String metastoreUris = "thrift://127.0.0.1:9083";
//        HiveMetadata metadata = new HiveMetadata(metastoreUris);
//        try {
//            Assert.assertNull(metadata.listTableNames(db2));
//        } catch (Exception e) {
//            Assert.assertTrue(e instanceof DdlException);
//        }
//    }
//
//    @Test
//    public void testGetTable(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws Exception {
//        String db = "db";
//        String tbl1 = "tbl1";
//        String tbl2 = "tbl2";
//        String resourceName = "thrift://127.0.0.1:9083";
//        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "BIGINT", ""));
//        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
//        String hdfsPath = "hdfs://127.0.0.1:10000/hive";
//        StorageDescriptor sd = new StorageDescriptor();
//        sd.setCols(unPartKeys);
//        sd.setLocation(hdfsPath);
//        Table msTable1 = new Table();
//        msTable1.setDbName(db);
//        msTable1.setTableName(tbl1);
//        msTable1.setPartitionKeys(partKeys);
//        msTable1.setSd(sd);
//        msTable1.setTableType("MANAGED_TABLE");
//
//        Table msTable2 = new Table();
//        msTable2.setDbName(db);
//        msTable2.setTableName(tbl2);
//        msTable2.setPartitionKeys(partKeys);
//        msTable2.setSd(sd);
//        msTable2.setTableType("VIRTUAL_VIEW");
//
//        new Expectations() {
//            {
//                metaStoreThriftClient.getTable(db, tbl1);
//                result = msTable1;
//
//                metaStoreThriftClient.getTable(db, tbl2);
//                result = msTable2;
//            }
//        };
//        HiveMetadata metadata = new HiveMetadata(resourceName);
//        HiveTable table1 = (HiveTable) metadata.getTable(db, tbl1);
//        Assert.assertEquals(tbl1, table1.getTableName());
//        Assert.assertEquals(db, table1.getHiveDb());
//        Assert.assertEquals(String.format("%s.%s", db, tbl1), table1.getHiveDbTable());
//        Assert.assertEquals(hdfsPath, table1.getHdfsPath());
//        Assert.assertEquals(Lists.newArrayList(new Column("col1", Type.BIGINT, true)), table1.getPartitionColumns());
//
//        HiveTable table1FromCache = (HiveTable) metadata.getTable(db, tbl1);
//        Assert.assertEquals(table1, table1FromCache);
//        // test "VIRTUAL_VIEW" type table
//        Assert.assertNull(metadata.getTable(db, tbl2));
//    }
//
//    @Test
//    public void testNotExistTable() throws DdlException {
//        String resourceName = "thrift://127.0.0.1:9083";
//        HiveMetadata metadata = new HiveMetadata(resourceName);
//        Assert.assertNull(metadata.getTable("db", "tbl"));
//    }
//
//}
