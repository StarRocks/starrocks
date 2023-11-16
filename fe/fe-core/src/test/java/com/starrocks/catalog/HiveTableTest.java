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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/HiveTableTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.HiveTableFactory;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static com.starrocks.server.ExternalTableFactory.RESOURCE;

public class HiveTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private HiveMetaClient hiveClient;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withResource("create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")");
        starRocksAssert.withDatabase("db");
        hiveClient = new HiveMetastoreTest.MockedHiveMetaClient();
    }

    com.starrocks.catalog.Table createTable(CreateTableStmt stmt) throws DdlException {
        return TableFactoryProvider.getFactory(EngineType.HIVE.name()).createTable(null, null, stmt);
    }

    @Test
    public void testCreateExternalTable(@Mocked MetadataMgr metadataMgr) throws Exception {
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
        msTable.setTableName("hive_table");
        msTable.setDbName("hive_db");
        int createTime = (int) System.currentTimeMillis();
        msTable.setCreateTime(createTime);

        HiveTable oTable =
                HiveMetastoreApiConverter.toHiveTable(msTable, getResourceMappingCatalogName("hive0", "hive"));
        Assert.assertTrue(oTable.supportInsert());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getTable(anyString, anyString, anyString);
                result = oTable;
            }
        };

        String createTableSql =
                "create external table if not exists  db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                        "(\"resource\"=\"hive0\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);

        Assert.assertTrue(table instanceof HiveTable);
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("hive_tbl", hiveTable.getName());
        Assert.assertEquals("hive_db", hiveTable.getDbName());
        Assert.assertEquals("hive_table", hiveTable.getTableName());
        Assert.assertEquals(hdfsPath, hiveTable.getTableLocation());
        Assert.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assert.assertEquals(new Column("col1", Type.INT, true), hiveTable.getPartitionColumns().get(0));
        Assert.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assert.assertFalse(hiveTable.isUnPartitioned());

    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws Exception {
        String createTableSql = "create external table nodb.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"resource\"=\"hive0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoTbl() throws Exception {
        String createTableSql = "create external table nodb.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"resource\"=\"hive0\")";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoResource() throws Exception {
        String createTableSql = "create external table db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"resource\"=\"not_exist_reousrce\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNotExistResource() throws Exception {
        String createTableSql = "create external table db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testHiveColumnConvert(@Mocked MetadataMgr metadataMgr) throws Exception {
        Table msTable = hiveClient.getTable("hive_db", "hive_table");
        HiveTable oTable =
                HiveMetastoreApiConverter.toHiveTable(msTable, getResourceMappingCatalogName("hive0", "hive"));
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getTable(anyString, anyString, anyString);
                result = oTable;
            }
        };

        String createTableSql = "create external table  if not exists  db.hive_tbl (col1 int, not_exist int) " +
                "engine=hive properties " +
                "(\"resource\"=\"hive0\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testHasBoolPartitionColumn() {
        Table msTable = hiveClient.getTable("hive_db", "hive_table");
        HiveTable oTable = HiveMetastoreApiConverter.toHiveTable(msTable, getResourceMappingCatalogName("hive0", "hive"));
        Assert.assertFalse(oTable.hasBooleanTypePartitionColumn());
    }

    // create a hive table with specific storage format
    private HiveTable createExternalTableByFormat(String format) throws Exception {
        String inputFormatClass = HiveStorageFormat.get(format).getInputFormat();
        String outputFormatClass = HiveStorageFormat.get(format).getOutputFormat();

        String serde = HiveStorageFormat.get(format).getSerde();
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib(serde);

        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "INT", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hive";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setInputFormat(inputFormatClass);
        sd.setOutputFormat(outputFormatClass);
        sd.setSerdeInfo(serDeInfo);

        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        Table msTable = new Table();
        msTable.setPartitionKeys(partKeys);
        msTable.setSd(sd);
        msTable.setTableType("MANAGED_TABLE");

        //        String tableName = format.toLowerCase() + "_table";
        String tableName = "hive_table";
        msTable.setTableName(tableName);
        msTable.setDbName("hive_db");
        int createTime = (int) System.currentTimeMillis();
        msTable.setCreateTime(createTime);

        HiveTable oTable =
                HiveMetastoreApiConverter.toHiveTable(msTable, getResourceMappingCatalogName("hive0", "hive"));
        return oTable;
    }

    @Test
    public void testCreateExternalTableWithStorageFormat(@Mocked MetadataMgr metadataMgr) throws Exception {

        List<String> targetFormats = new ArrayList<>();
        targetFormats.add("AVRO");
        targetFormats.add("RCBINARY");
        targetFormats.add("RCTEXT");
        targetFormats.add("SEQUENCE");

        for (String targetFormat : targetFormats) {
            HiveTable oTable = createExternalTableByFormat(targetFormat);
            String inputFormatClass = HiveStorageFormat.get(targetFormat).getInputFormat();
            String serde = HiveStorageFormat.get(targetFormat).getSerde();

            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState().getMetadataMgr();
                    result = metadataMgr;
                    minTimes = 0;

                    metadataMgr.getTable(anyString, anyString, anyString);
                    result = oTable;
                }
            };

            String createTableSql =
                    "create external table if not exists  db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                            "(\"resource\"=\"hive0\", \"database\"=\"db0\", \"table\"=\"table0\")";
            CreateTableStmt createTableStmt =
                    (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
            com.starrocks.catalog.Table table = createTable(createTableStmt);

            Assert.assertTrue(table instanceof HiveTable);
            HiveTable hiveTable = (HiveTable) table;
            List<DescriptorTable.ReferencedPartitionInfo> partitions = new ArrayList<>();
            TTableDescriptor tTableDescriptor = hiveTable.toThrift(partitions);

            Assert.assertEquals(tTableDescriptor.getHdfsTable().getInput_format(), inputFormatClass);
            Assert.assertEquals(tTableDescriptor.getHdfsTable().getSerde_lib(), serde);
            Assert.assertEquals(tTableDescriptor.getHdfsTable().getHive_column_names(), "col2");
            Assert.assertEquals(tTableDescriptor.getHdfsTable().getHive_column_types(), "INT");
        }
    }

    @Test
    public void testCreateTableResourceName() throws DdlException {

        String resourceName = "Hive_resource_29bb53dc_7e04_11ee_9b35_00163e0e489a";
        Map<String, String> properties = new HashMap() {
            {
                put(RESOURCE, resourceName);
            }
        };
        HiveTable.Builder tableBuilder = HiveTable.builder()
                .setId(1000)
                .setTableName("supplier")
                .setCatalogName("hice_catalog")
                .setHiveDbName("hive_oss_tpch_1g_parquet_gzip")
                .setHiveTableName("supplier")
                .setResourceName(resourceName)
                .setTableLocation("")
                .setFullSchema(new ArrayList<>())
                .setDataColumnNames(new ArrayList<>())
                .setPartitionColumnNames(Lists.newArrayList())
                .setStorageFormat(null);
        HiveTable oTable = tableBuilder.build();
        HiveTable.Builder newBuilder = HiveTable.builder();
        HiveTableFactory.copyFromCatalogTable(newBuilder, oTable, properties);
        HiveTable table = newBuilder.build();
        Assert.assertEquals(table.getResourceName(), resourceName);
    }
}
