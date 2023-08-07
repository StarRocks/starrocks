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


package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveClassNames;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.HudiTable.HUDI_BASE_PATH;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_NAMES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_TYPES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_INPUT_FOAMT;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_SERDE_LIB;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_TYPE;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

public class HiveMetastoreApiConverterTest {
    Schema hudiSchema;

    @Before
    public void setup() {
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
        hudiSchema = Schema.createRecord(hudiFields);
    }

    @Test
    public void testToFullSchemasForHudiTable() {
        List<Column> columns = HiveMetastoreApiConverter.toFullSchemasForHudiTable(hudiSchema);
        Assert.assertEquals(7, columns.size());
    }

    @Test
    public void testToDataColumnNamesForHudiTable() {
        List<String> partColumns = Lists.newArrayList("col1");
        List<String> dataColumns = HiveMetastoreApiConverter.toDataColumnNamesForHudiTable(hudiSchema, partColumns);
        Assert.assertEquals(6, dataColumns.size());
    }

    @Test
    public void testToHudiProperties(@Mocked Table table, @Mocked HoodieTableMetaClient metaClient) {
        StorageDescriptor sd = new StorageDescriptor();
        String tableLocation = "hdfs://127.0.0.1/db/table/hudi_table";
        String serLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
        String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
        sd.setLocation(tableLocation);

        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "bigint", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList();
        unPartKeys.add(new FieldSchema("_hoodie_commit_time", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_commit_seqno", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_record_key", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_partition_path", "string", ""));
        unPartKeys.add(new FieldSchema("_hoodie_file_name", "string", ""));
        unPartKeys.add(new FieldSchema("col2", "int", ""));

        new Expectations() {
            {
                table.getSd().getLocation();
                result = tableLocation;

                table.getSd().getSerdeInfo().getSerializationLib();
                result = serLib;

                table.getSd().getInputFormat();
                result = inputFormat;

                metaClient.getTableType().name();
                result = COPY_ON_WRITE;

                table.getSd().getCols();
                result = unPartKeys;

                table.getPartitionKeys();
                result = partKeys;
            }
        };

        Map<String, String> params = HiveMetastoreApiConverter.toHudiProperties(table, metaClient, hudiSchema);
        Assert.assertEquals(tableLocation, params.get(HUDI_BASE_PATH));
        Assert.assertEquals(serLib, params.get(HUDI_TABLE_SERDE_LIB));
        Assert.assertEquals(inputFormat, params.get(HUDI_TABLE_INPUT_FOAMT));
        Assert.assertEquals("COPY_ON_WRITE", params.get(HUDI_TABLE_TYPE));
        Assert.assertEquals("_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key," +
                "_hoodie_partition_path,_hoodie_file_name,col1,col2", params.get(HUDI_TABLE_COLUMN_NAMES));
        Assert.assertEquals("string#string#string#string#string#bigint#int", params.get(HUDI_TABLE_COLUMN_TYPES));
    }

    @Test
    public void testValidateTableType() {
        try {
            HiveMetastoreApiConverter.validateHiveTableType(null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof  StarRocksConnectorException);
        }

        try {
            HiveMetastoreApiConverter.validateHiveTableType("xxxx");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof  StarRocksConnectorException);
        }

        try {
            HiveMetastoreApiConverter.validateHiveTableType("VIRTUAL_VIEW");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testToMetastoreApiTable() {
        HiveTable hiveTable = HiveTable.builder()
                .setCatalogName("hive_catalog")
                .setHiveDbName("hive_db")
                .setHiveTableName("hive_table")
                .setPartitionColumnNames(Lists.newArrayList("p1"))
                .setFullSchema(Lists.newArrayList(new Column("c1", Type.INT), new Column("p1", Type.INT)))
                .setDataColumnNames(Lists.newArrayList("c1"))
                .setTableLocation("table_location")
                .build();
        hiveTable.setComment("my_comment");
        Table table = HiveMetastoreApiConverter.toMetastoreApiTable(hiveTable);
        Assert.assertEquals("hive_table", table.getTableName());
        Assert.assertEquals("hive_db", table.getDbName());
        Assert.assertEquals("p1", table.getPartitionKeys().get(0).getName());
        Assert.assertEquals("table_location", table.getSd().getLocation());
        Assert.assertEquals("c1", table.getSd().getCols().get(0).getName());
        Assert.assertEquals("int", table.getSd().getCols().get(0).getType());

        Assert.assertEquals(HiveClassNames.PARQUET_HIVE_SERDE_CLASS, table.getSd().getSerdeInfo().getSerializationLib());
        Assert.assertEquals(HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS, table.getSd().getInputFormat());
        Assert.assertEquals(HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS, table.getSd().getOutputFormat());

        Assert.assertEquals("my_comment", table.getParameters().get("comment"));
        Assert.assertEquals("-1", table.getParameters().get("numFiles"));
    }
}
