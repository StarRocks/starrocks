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

import com.google.common.collect.Lists;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PaimonTableTest {

    @Test
    public void testPartitionKeys(@Mocked FileStoreTable paimonNativeTable) {
        RowType rowType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).field("c", DataTypes.INT())
                        .build();
        List<DataField> fields = rowType.getFields();
        List<Column> fullSchema = new ArrayList<>(fields.size());
        ArrayList<String> partitions = Lists.newArrayList("b", "c");

        ArrayList<Column> partitionSchema = new ArrayList<>();
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true);
            fullSchema.add(column);
            if (partitions.contains(fieldName)) {
                partitionSchema.add(column);
            }
        }
        new Expectations() {
            {
                paimonNativeTable.rowType();
                result = rowType;
                paimonNativeTable.partitionKeys();
                result = partitions;
            }
        };
        PaimonTable paimonTable = new PaimonTable("testCatalog", "testDB", "testTable", fullSchema, paimonNativeTable);
        Map<String, String> properties = paimonTable.getProperties();
        org.junit.jupiter.api.Assertions.assertEquals(0, properties.size());
        List<Column> partitionColumns = paimonTable.getPartitionColumns();
        Assertions.assertThat(partitionColumns).hasSameElementsAs(partitionSchema);
    }

    @Test
    public void testToThrift(@Mocked FileStoreTable paimonNativeTable) {
        RowType rowType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).field("c", DataTypes.INT())
                        .build();
        List<DataField> fields = rowType.getFields();
        List<Column> fullSchema = new ArrayList<>(fields.size());
        ArrayList<String> partitions = Lists.newArrayList("b", "c");
        new Expectations() {
            {
                paimonNativeTable.rowType();
                result = rowType;
                paimonNativeTable.partitionKeys();
                result = partitions;
            }
        };
        String dbName = "testDB";
        String tableName = "testTable";
        PaimonTable paimonTable = new PaimonTable("testCatalog", dbName, tableName, fullSchema, paimonNativeTable);

        TTableDescriptor tTableDescriptor = paimonTable.toThrift(null);
        org.junit.jupiter.api.Assertions.assertEquals(tTableDescriptor.getDbName(), dbName);
        org.junit.jupiter.api.Assertions.assertEquals(tTableDescriptor.getTableName(), tableName);
    }

    @Test
    public void testEquals(@Mocked FileStoreTable paimonNativeTable) {
        String dbName = "testDB";
        String tableName = "testTable";
        PaimonTable table = new PaimonTable("testCatalog", dbName, tableName, null, paimonNativeTable);
        PaimonTable table2 = new PaimonTable("testCatalog", dbName, tableName, null, paimonNativeTable);
        org.junit.jupiter.api.Assertions.assertEquals(table, table2);
        org.junit.jupiter.api.Assertions.assertEquals(table, table);
        org.junit.jupiter.api.Assertions.assertNotEquals(table, null);
    }

    @Test
    public void testGetUUID() throws Exception {
        //1.initialize env、db、table
        java.nio.file.Path tmpDir = Files.createTempDirectory("tmp_");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        PaimonMetadata metadata = new PaimonMetadata("paimon_catalog", new HdfsEnvironment(), catalog,
                new ConnectorProperties(ConnectorType.PAIMON));

        catalog.createDatabase("test_db", true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("order_id", DataTypes.STRING());
        schemaBuilder.column("order_date", DataTypes.STRING());
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 2);
        options.set(CoreOptions.BUCKET_KEY, "order_id");
        schemaBuilder.options(options.toMap());
        Schema schema = schemaBuilder.build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        //2.check
        //2.1 check not system table
        PaimonTable paimonNotSystemTable = (PaimonTable) metadata.getTable(connectContext, "test_db", "test_table");
        org.junit.jupiter.api.Assertions.assertEquals(paimonNotSystemTable.getUUID().split("\\.").length, 4);
        org.junit.jupiter.api.Assertions.assertTrue(
                paimonNotSystemTable.getUUID().contains("paimon_catalog.test_db.test_table"));

        //2.2 check system table
        PaimonTable paimonSystemTable = (PaimonTable) metadata.getTable(connectContext, "test_db", "test_table$snapshots");
        org.junit.jupiter.api.Assertions.assertEquals(paimonSystemTable.getUUID().split("\\.").length, 4);
        org.junit.jupiter.api.Assertions.assertTrue(
                paimonSystemTable.getUUID().contains("paimon_catalog.test_db.test_table$snapshots"));

        //3.clean env
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }
}
