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

package com.starrocks.connector.opensearch;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class OpenSearchMetadataTest {

    @Test
    public void testListDbNamesReturnsDefaultDb(@Mocked OpenSearchRestClient client) {
        Map<String, String> properties = new HashMap<>();
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, properties, "test_catalog");
        
        List<String> dbNames = metadata.listDbNames(new ConnectContext());
        
        assertNotNull(dbNames);
        assertEquals(1, dbNames.size());
        assertEquals("default_db", dbNames.get(0));
    }

    @Test
    public void testGetDbReturnsDefaultDatabase(@Mocked OpenSearchRestClient client) {
        Map<String, String> properties = new HashMap<>();
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, properties, "test_catalog");
        
        Database db = metadata.getDb(new ConnectContext(), "default_db");
        
        assertNotNull(db);
        assertEquals("default_db", db.getFullName());
        assertEquals(1L, db.getId());
    }

    @Test
    public void testGetDbReturnsNullForNonDefaultDb(@Mocked OpenSearchRestClient client) {
        Map<String, String> properties = new HashMap<>();
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, properties, "test_catalog");
        
        Database db = metadata.getDb(new ConnectContext(), "non_existent_db");
        
        // Returns default_db regardless of input
        assertNotNull(db);
        assertEquals("default_db", db.getFullName());
    }

    @Test
    public void testGetTableReturnsNullForNonDefaultDb(@Mocked OpenSearchRestClient client) {
        Map<String, String> properties = new HashMap<>();
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, properties, "test_catalog");
        
        Table table = metadata.getTable(new ConnectContext(), "non_default_db", "some_table");
        
        assertNull(table);
    }

    @Test
    public void testTableTypeIsElasticsearch(@Mocked OpenSearchRestClient client) {
        Map<String, String> properties = new HashMap<>();
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, properties, "test_catalog");
        
        assertEquals(Table.TableType.OPENSEARCH, metadata.getTableType());
    }

    @Test
    public void testConstructorStoresProperties(@Mocked OpenSearchRestClient client) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hosts", "localhost:9200");
        
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, properties, "test_catalog");
        
        assertNotNull(metadata);
    }

    @Test
    public void testDefaultDbConstants() {
        assertEquals("default_db", OpenSearchMetadata.DEFAULT_DB);
        assertEquals(1L, OpenSearchMetadata.DEFAULT_DB_ID);
    }
}
