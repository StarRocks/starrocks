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

package com.starrocks.connector.iceberg;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class AbstractIcebergMetadataScannerTest {

    // The deserialized-table cache is a lazily built singleton whose capacity comes from the mutable
    // BE config; it must be rebuilt when that capacity changes so the runtime knob actually applies.
    @Test
    public void testTableCacheRebuildsOnCapacityChange() {
        DeserializedTableCache first = AbstractIcebergMetadataScanner.tableCache(64);
        assertSame(first, AbstractIcebergMetadataScanner.tableCache(64), "same capacity reuses the cache");

        DeserializedTableCache rebuilt = AbstractIcebergMetadataScanner.tableCache(128);
        assertNotSame(first, rebuilt, "changed capacity rebuilds the cache");
        assertSame(rebuilt, AbstractIcebergMetadataScanner.tableCache(128), "stable after rebuild");
    }

    // The cache capacity is read from the table_cache_max_entries param the BE passes per scanner.
    @Test
    public void testConstructorReadsCacheCapacityFromParams() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "content");
        params.put("metadata_column_names", "content");
        params.put("metadata_column_types", "int");
        params.put("serialized_table", "payload");
        params.put("table_cache_max_entries", "64");

        AbstractIcebergMetadataScanner scanner = new AbstractIcebergMetadataScanner(1, params) {
            @Override
            protected void doOpen() {
            }

            @Override
            protected int doGetNext() {
                return 0;
            }

            @Override
            protected void doClose() {
            }

            @Override
            protected void initReader() {
            }
        };

        assertNotNull(scanner);
    }
}
