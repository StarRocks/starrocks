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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.common.EngineType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;

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

        HiveTable oTable = HiveMetastoreApiConverter.toHiveTable(msTable, getResourceMappingCatalogName("hive0", "hive"));
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getTable(anyString, anyString, anyString);
                result = oTable;
            }
        };

        String createTableSql = "create external table if not exists  db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"resource\"=\"hive0\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
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
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoTbl() throws Exception {
        String createTableSql = "create external table nodb.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"resource\"=\"hive0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoResource() throws Exception {
        String createTableSql = "create external table db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"resource\"=\"not_exist_reousrce\", \"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNotExistResource() throws Exception {
        String createTableSql = "create external table db.hive_tbl (col1 int, col2 int) engine=hive properties " +
                "(\"database\"=\"db0\", \"table\"=\"table0\")";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testHiveColumnConvert(@Mocked MetadataMgr metadataMgr) throws Exception {
        Table msTable = hiveClient.getTable("hive_db", "hive_table");
        HiveTable oTable = HiveMetastoreApiConverter.toHiveTable(msTable, getResourceMappingCatalogName("hive0", "hive"));
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
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, connectContext);
        com.starrocks.catalog.Table table = createTable(createTableStmt);
        Assert.fail("No exception throws.");
    }
}
