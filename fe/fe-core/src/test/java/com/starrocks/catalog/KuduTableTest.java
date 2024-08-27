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

import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.kudu.ColumnSchema;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KuduTableTest {

    @Test
    public void testPartitionKeys() {
        List<ColumnSchema> columns = Arrays.asList(
                genColumnSchema("a", org.apache.kudu.Type.INT32),
                genColumnSchema("b", org.apache.kudu.Type.STRING),
                genColumnSchema("c", org.apache.kudu.Type.DATE)
        );
        List<Column> fullSchema = new ArrayList<>(columns.size());
        List<String> partColNames = Arrays.asList("a", "b");
        ArrayList<Column> partitionSchema = new ArrayList<>();
        for (ColumnSchema column : columns) {
            Type fieldType = ColumnTypeConverter.fromKuduType(column);
            Column convertedColumn = new Column(column.getName(), fieldType, true);
            fullSchema.add(convertedColumn);
            if (partColNames.contains(column.getName())) {
                partitionSchema.add(convertedColumn);
            }
        }
        String catalogName = "testCatalog";
        String dbName = "testDB";
        String tableName = "testTable";
        String kuduTableName = "impala::testDB.testTable";
        KuduTable kuduTable = new KuduTable("localhost:7051", catalogName, dbName, tableName,
                kuduTableName, fullSchema, partColNames);
        List<Column> partitionColumns = kuduTable.getPartitionColumns();
        Assertions.assertThat(partitionColumns).hasSameElementsAs(partitionSchema);
    }

    @Test
    public void testToThrift() {
        List<ColumnSchema> columns = Arrays.asList(
                genColumnSchema("a", org.apache.kudu.Type.INT32),
                genColumnSchema("b", org.apache.kudu.Type.STRING),
                genColumnSchema("c", org.apache.kudu.Type.DATE)
        );
        List<Column> fullSchema = new ArrayList<>(columns.size());
        for (ColumnSchema column : columns) {
            Type fieldType = ColumnTypeConverter.fromKuduType(column);
            Column convertedColumn = new Column(column.getName(), fieldType, true);
            fullSchema.add(convertedColumn);
        }
        String catalogName = "testCatalog";
        String dbName = "testDB";
        String tableName = "testTable";
        KuduTable kuduTable = new KuduTable("localhost:7051", catalogName, dbName, tableName,
                null, fullSchema, new ArrayList<>());

        TTableDescriptor tTableDescriptor = kuduTable.toThrift(null);
        Assert.assertEquals(tTableDescriptor.getDbName(), dbName);
        Assert.assertEquals(tTableDescriptor.getTableName(), tableName);
        Assert.assertEquals(tTableDescriptor.getNumCols(), 3);
        Assert.assertEquals(tTableDescriptor.getTableType(), TTableType.KUDU_TABLE);
    }

    public static ColumnSchema genColumnSchema(String name, org.apache.kudu.Type type) {
        return new ColumnSchema.ColumnSchemaBuilder(name, type).build();
    }
}