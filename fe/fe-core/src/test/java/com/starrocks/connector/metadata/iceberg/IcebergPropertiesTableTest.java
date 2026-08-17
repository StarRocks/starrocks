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

package com.starrocks.connector.metadata.iceberg;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergPropertiesTableTest {

    @Test
    public void testCreate() {
        IcebergPropertiesTable table = IcebergPropertiesTable.create("test_catalog", "test_db", "test_table");

        assertNotNull(table);
        assertEquals("iceberg_properties_table", table.getName());
        assertEquals(Table.TableType.METADATA, table.getType());
        assertEquals(MetadataTableType.PROPERTIES, table.getMetadataTableType());
        assertEquals("test_db", table.getOriginDb());
        assertEquals("test_table", table.getOriginTable());
        assertEquals("test_catalog", table.getCatalogName());
    }

    @Test
    public void testSchema() {
        IcebergPropertiesTable table = IcebergPropertiesTable.create("test_catalog", "test_db", "test_table");
        List<Column> columns = table.getFullSchema();

        assertEquals(2, columns.size());
        assertEquals("key", columns.get(0).getName());
        assertEquals("value", columns.get(1).getName());
    }

    @Test
    public void testToThrift() {
        IcebergPropertiesTable table = IcebergPropertiesTable.create("test_catalog", "test_db", "test_table");
        TTableDescriptor descriptor = table.toThrift(null);

        assertNotNull(descriptor);
        assertEquals(TTableType.ICEBERG_PROPERTIES_TABLE, descriptor.getTableType());
        assertTrue(descriptor.isSetHdfsTable());
    }

    @Test
    public void testSupportBuildPlan() {
        IcebergPropertiesTable table = IcebergPropertiesTable.create("test_catalog", "test_db", "test_table");
        assertTrue(table.supportBuildPlan());
    }
}
