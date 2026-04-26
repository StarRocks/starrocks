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
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetadataTableTypeTest {

    @Test
    public void testGetPropertiesType() {
        MetadataTableType type = MetadataTableType.get("properties");
        assertEquals(MetadataTableType.PROPERTIES, type);
        assertEquals("properties", type.typeString);
    }

    @Test
    public void testGetPropertiesTypeCaseInsensitive() {
        MetadataTableType type = MetadataTableType.get("PROPERTIES");
        assertEquals(MetadataTableType.PROPERTIES, type);
    }

    @Test
    public void testGetAllTypes() {
        assertEquals(MetadataTableType.LOGICAL_ICEBERG_METADATA, MetadataTableType.get("logical_iceberg_metadata"));
        assertEquals(MetadataTableType.REFS, MetadataTableType.get("refs"));
        assertEquals(MetadataTableType.HISTORY, MetadataTableType.get("history"));
        assertEquals(MetadataTableType.METADATA_LOG_ENTRIES, MetadataTableType.get("metadata_log_entries"));
        assertEquals(MetadataTableType.SNAPSHOTS, MetadataTableType.get("snapshots"));
        assertEquals(MetadataTableType.MANIFESTS, MetadataTableType.get("manifests"));
        assertEquals(MetadataTableType.FILES, MetadataTableType.get("files"));
        assertEquals(MetadataTableType.PARTITIONS, MetadataTableType.get("partitions"));
        assertEquals(MetadataTableType.PROPERTIES, MetadataTableType.get("properties"));
    }

    @Test
    public void testGetUnknownTypeThrows() {
        assertThrows(IllegalArgumentException.class, () -> MetadataTableType.get("unknown_type"));
    }
}
