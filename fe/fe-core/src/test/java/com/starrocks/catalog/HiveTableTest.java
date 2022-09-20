// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class HiveTableTest {
    private String hiveDb;
    private String hiveTable;
    String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        hiveDb = "db0";
        hiveTable = "table0";
        resourceName = "hive0";

        columns = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT, true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("database", hiveDb);
        properties.put("table", hiveTable);
        properties.put("resource", resourceName);
    }

    @Test
    public void testWithResourceName(@Mocked GlobalStateMgr globalStateMgr,
                                     @Mocked ResourceMgr resourceMgr,
                                     @Mocked HiveRepository hiveRepository) throws DdlException {
        Resource hiveResource = new HiveResource(resourceName);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        hiveResource.setProperties(resourceProperties);

        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "BIGINT", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hive";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        Table msTable = new Table();
        msTable.setPartitionKeys(partKeys);
        msTable.setSd(sd);
        msTable.setTableType("MANAGED_TABLE");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("hive0");
                result = hiveResource;

                globalStateMgr.getHiveRepository();
                result = hiveRepository;

                hiveRepository.getTable(resourceName, hiveDb, hiveTable);
                result = msTable;
            }
        };

        columns.add(new Column("col2", Type.INT, true));
        properties.put("resource", resourceName);
        HiveTable table = new HiveTable(1000, "hive_table", columns, properties);
        Assert.assertEquals(hiveTable, table.getTableName());
        Assert.assertEquals(hiveDb, table.getDbName());
        Assert.assertEquals(String.format("%s.%s", hiveDb, hiveTable), table.getHiveDbTable());
        Assert.assertEquals(hdfsPath, table.getHdfsPath());
        Assert.assertEquals(Lists.newArrayList(new Column("col1", Type.BIGINT, true)), table.getPartitionColumns());
    }

    @Test(expected = DdlException.class)
    public void testNoDb() throws DdlException {
        properties.remove("database");
        new HiveTable(1000, "hive_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws DdlException {
        properties.remove("table");
        new HiveTable(1000, "hive_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoHiveMetastoreUris() throws DdlException {
        properties.remove("hive.metastore.uris");
        new HiveTable(1000, "hive_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testHiveColumnConvert() throws DdlException {
        // test case for hiveType from hms
        Assert.assertTrue(HiveMetaStoreTableUtils.validateColumnType("BINARY", Type.UNKNOWN_TYPE));
        Assert.assertTrue(HiveMetaStoreTableUtils.validateColumnType("uniontype<int,double>", Type.UNKNOWN_TYPE));
        Assert.assertTrue(HiveMetaStoreTableUtils.validateColumnType("array<struct<col_int:int>>", Type.UNKNOWN_TYPE));
        ScalarType keyType = ScalarType.createType(PrimitiveType.INT);
        ScalarType valueType = ScalarType.createType(PrimitiveType.BIGINT);
        MapType mapType = new MapType(keyType, valueType);
        Assert.assertTrue(HiveMetaStoreTableUtils.validateColumnType("map<int,bigint>", mapType));
        Assert.assertTrue(HiveMetaStoreTableUtils.validateColumnType("struct<col_int:int>", Type.UNKNOWN_TYPE));
        // test case for hiveType mis input
        Assert.assertEquals(Type.UNKNOWN_TYPE,
                HiveMetaStoreTableUtils.convertHiveTableColumnType("array<brray<int>>"));
    }
}
