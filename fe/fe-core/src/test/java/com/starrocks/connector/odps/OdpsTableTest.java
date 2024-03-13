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

package com.starrocks.connector.odps;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OdpsTable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OdpsTableTest {

    @Test
    public void testGetResourceName() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        when(odpsTable.getResourceName()).thenReturn("tableName");
        assertEquals("tableName", odpsTable.getResourceName());
    }

    @Test
    public void testGetCatalogName() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        when(odpsTable.getCatalogName()).thenReturn("catalogName");
        assertEquals("catalogName", odpsTable.getCatalogName());
    }

    @Test
    public void testGetDbName() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        when(odpsTable.getDbName()).thenReturn("dbName");
        assertEquals("dbName", odpsTable.getDbName());
    }

    @Test
    public void testGetTableName() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        when(odpsTable.getTableName()).thenReturn("tableName");
        assertEquals("tableName", odpsTable.getTableName());
    }

    @Test
    public void testDataColumnNames() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        List<Column> dataColumns = new ArrayList<>();
        Column column = new Column();
        column.setName("name");
        dataColumns.add(column);
        when(odpsTable.getDataColumnNames()).thenReturn(
                dataColumns.stream().map(Column::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList("name"), odpsTable.getDataColumnNames());
    }

    @Test
    public void testPartitionColumns() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        List<Column> partitionColumns = new ArrayList<>();
        Column column = new Column();
        column.setName("name");
        partitionColumns.add(column);
        when(odpsTable.getPartitionColumns()).thenReturn(partitionColumns);
        assertEquals(partitionColumns, odpsTable.getPartitionColumns());
    }

    @Test
    public void testPartitionColumnNames() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        List<Column> partitionColumns = new ArrayList<>();
        Column column = new Column();
        column.setName("name");
        partitionColumns.add(column);
        when(odpsTable.getPartitionColumnNames()).thenReturn(
                partitionColumns.stream().map(Column::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList("name"), odpsTable.getPartitionColumnNames());
    }

    @Test
    public void testGetProperty() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        Map<String, String> properties = new HashMap<>();
        properties.put("propertyKey", "propertyValue");
        when(odpsTable.getProperty("propertyKey")).thenReturn(properties.get("propertyKey"));
        assertEquals("propertyValue", odpsTable.getProperty("propertyKey"));
    }

    @Test
    public void testGetUUID() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        when(odpsTable.getUUID()).thenReturn(String.join(".", "catalogName", "dbName", "name", Long.toString(1)));
        assertEquals("catalogName.dbName.name.1", odpsTable.getUUID());
    }

    @Test
    public void testIsSupported() {
        OdpsTable odpsTable = mock(OdpsTable.class);
        when(odpsTable.isSupported()).thenReturn(true);
        assertEquals(true, odpsTable.isSupported());
    }
}
