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
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HudiTableTest {
    private String hudiDb;
    private String hudiTable;
    String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        hudiDb = "db0";
        hudiTable = "table0";
        resourceName = "hudi0";

        columns = Lists.newArrayList();
        columns.add(new Column("col1", Type.BIGINT, true));
        columns.add(new Column("col2", Type.INT, true));

        properties = Maps.newHashMap();
        properties.put("database", hudiDb);
        properties.put("table", hudiTable);
        properties.put("resource", resourceName);
    }

    @Test
    public void testWithResourceName(@Mocked GlobalStateMgr globalStateMgr,
                                     @Mocked ResourceMgr resourceMgr,
                                     @Mocked HiveRepository hiveRepository,
                                     @Mocked HoodieTableMetaClient metaClient,
                                     @Mocked TableSchemaResolver schemaUtil) throws Exception {
        Resource hudiResource = new HudiResource(resourceName);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        hudiResource.setProperties(resourceProperties);

        List<Schema.Field> hudiFields = new ArrayList<>();
        hudiFields.add(new Schema.Field("_hoodie_commit_time",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null));
        hudiFields.add(new Schema.Field("_hoodie_commit_seqno",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null));
        hudiFields.add(new Schema.Field("_hoodie_record_key",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null));
        hudiFields.add(new Schema.Field("_hoodie_partition_path",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null));
        hudiFields.add(new Schema.Field("_hoodie_file_name",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null));
        hudiFields.add(new Schema.Field("col1",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)), "", null));
        hudiFields.add(new Schema.Field("col2",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)), "", null));
        Schema hudiSchema = Schema.createRecord(hudiFields);

        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "bigint", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList();
        unPartKeys.add(new FieldSchema("_hoodie_commit_time", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_commit_seqno", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_record_key", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_partition_path", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_file_name", "string", ""));
        unPartKeys.add(new FieldSchema("col2", "int", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hudi";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        sd.setInputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        SerDeInfo sdInfo = new SerDeInfo();
        sdInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        sd.setSerdeInfo(sdInfo);
        Table msTable = new Table();
        msTable.setPartitionKeys(partKeys);
        msTable.setSd(sd);
        msTable.setTableType("MANAGED_TABLE");
        msTable.setParameters(new HashMap<>());

        String[] hudiPartFields = new String[] {"col1"};

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                resourceMgr.getResource("hudi0");
                result = hudiResource;

                hiveRepository.getTable(resourceName, hudiDb, hudiTable);
                result = msTable;

                schemaUtil.getTableAvroSchema();
                result = hudiSchema;

                metaClient.getTableConfig().getPartitionFields();
                result = Option.of(hudiPartFields);
            }
        };
        HudiTable table = null;
        try {
            table = new HudiTable(1000, "hudi_table", columns, properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertEquals(hudiTable, table.getTable());
        Assert.assertEquals(hudiDb, table.getDbName());
        Assert.assertEquals(hdfsPath, table.getHudiBasePath());
        Assert.assertEquals(Lists.newArrayList(new Column("col1", Type.BIGINT, true)), table.getPartitionColumns());
    }

    @Test(expected = DdlException.class)
    public void testNoDb() throws DdlException {
        properties.remove("database");
        new HudiTable(1000, "hudi_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws DdlException {
        properties.remove("table");
        new HudiTable(1000, "hudi_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoHiveMetastoreUris() throws DdlException {
        properties.remove("hive.metastore.uris");
        new HudiTable(1000, "hudi_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNonNullAbleColumn() throws DdlException {
        List<Column> columns1 = Lists.newArrayList();
        columns1.add(new Column("col3", Type.INT, false));
        new HudiTable(1000, "hudi_table", columns1, properties);
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
    public void testColumnTypeConvert() throws DdlException {
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(Schema.create(Schema.Type.BOOLEAN)),
                ScalarType.createType(PrimitiveType.BOOLEAN));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(Schema.create(Schema.Type.INT)),
                ScalarType.createType(PrimitiveType.INT));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(Schema.create(Schema.Type.FLOAT)),
                ScalarType.createType(PrimitiveType.FLOAT));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(Schema.create(Schema.Type.DOUBLE)),
                ScalarType.createType(PrimitiveType.DOUBLE));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(Schema.create(Schema.Type.STRING)),
                ScalarType.createDefaultString());
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(
                Schema.createArray(Schema.create(Schema.Type.INT))),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(
                Schema.createFixed("FIXED", "FIXED", "F", 1)),
                ScalarType.createType(PrimitiveType.VARCHAR));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(
                Schema.createUnion(Schema.create(Schema.Type.INT))),
                ScalarType.createType(PrimitiveType.INT));
        Assert.assertEquals(HiveMetaStoreTableUtils.convertHudiTableColumnType(
                Schema.createMap(Schema.create(Schema.Type.INT))),
                ScalarType.createType(PrimitiveType.UNKNOWN_TYPE));
    }
}
