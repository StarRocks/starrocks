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

package com.starrocks.connector.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataTableNameTest {

    @Test
    public void testParsePropertiesTable() {
        MetadataTableName name = MetadataTableName.from("test_table$properties");
        assertEquals("test_table", name.getTableName());
        assertEquals(MetadataTableType.PROPERTIES, name.getTableType());
    }

    @Test
    public void testIsMetadataTableProperties() {
        assertTrue(MetadataTableName.isMetadataTable("test_table$properties"));
        assertTrue(MetadataTableName.isMetadataTable("my_iceberg_table$properties"));
    }

    @Test
    public void testIsMetadataTableAllTypes() {
        assertTrue(MetadataTableName.isMetadataTable("t$logical_iceberg_metadata"));
        assertTrue(MetadataTableName.isMetadataTable("t$refs"));
        assertTrue(MetadataTableName.isMetadataTable("t$history"));
        assertTrue(MetadataTableName.isMetadataTable("t$metadata_log_entries"));
        assertTrue(MetadataTableName.isMetadataTable("t$snapshots"));
        assertTrue(MetadataTableName.isMetadataTable("t$manifests"));
        assertTrue(MetadataTableName.isMetadataTable("t$files"));
        assertTrue(MetadataTableName.isMetadataTable("t$partitions"));
        assertTrue(MetadataTableName.isMetadataTable("t$properties"));
    }

    @Test
    public void testIsNotMetadataTable() {
        assertFalse(MetadataTableName.isMetadataTable("regular_table"));
        assertFalse(MetadataTableName.isMetadataTable("table$unknown_type"));
    }

    @Test
    public void testGetTableNameWithType() {
        MetadataTableName name = new MetadataTableName("my_table", MetadataTableType.PROPERTIES);
        assertEquals("my_table$properties", name.getTableNameWithType());
    }

    @Test
    public void testToString() {
        MetadataTableName name = MetadataTableName.from("iceberg_table$properties");
        assertEquals("iceberg_table$properties", name.toString());
    }
}
