package com.starrocks.external;

 import com.google.common.collect.Lists;
 import org.apache.hadoop.hive.metastore.api.FieldSchema;
 import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
 import org.apache.hadoop.hive.metastore.api.Table;
 import org.junit.Assert;
 import org.junit.Test;

 import java.util.List;

 import static com.starrocks.external.HiveMetaStoreTableUtils.getAllColumns;

 public class HiveMetaStoreTableUtilsTest {
     @Test
     public void testGetColumns() {
         String db = "db";
         String tbl1 = "tbl1";
         List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "bigint", ""));
         List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "binary", ""));
         String hdfsPath = "hdfs://127.0.0.1:10000/hive";
         StorageDescriptor sd = new StorageDescriptor();
         sd.setCols(unPartKeys);
         sd.setLocation(hdfsPath);
         Table msTable1 = new Table();
         msTable1.setDbName(db);
         msTable1.setTableName(tbl1);
         msTable1.setPartitionKeys(partKeys);
         msTable1.setSd(sd);
         msTable1.setTableType("MANAGED_TABLE");

         List<FieldSchema> allColumns = getAllColumns(msTable1);

         Assert.assertEquals(allColumns.size(), 2);
     }
 }