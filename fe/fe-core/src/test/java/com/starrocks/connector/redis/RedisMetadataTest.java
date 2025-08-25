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

package com.starrocks.connector.redis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RedisMetadataTest {

    private Map<String, String> properties;
    private RedisMetadata metadata;

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>();
        properties.put("redis.table-description-dir", ClassLoader.getSystemClassLoader().getResource("data/redis").getPath());
        metadata = new RedisMetadata(properties, "redis_catalog");
    }

    @Test
    public void testListDbNames() {
        try {
            List<String> result = metadata.listDbNames(new ConnectContext());
            List<String> expectResult = Lists.newArrayList("testdb");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testGetDb() {
        try {
            Database db = metadata.getDb(new ConnectContext(), "testdb");
            Assertions.assertEquals("testdb", db.getOriginName());
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testListTableNames() {
        try {
            List<String> result = metadata.listTableNames(new ConnectContext(), "testdb");
            List<String> expectResult = Lists.newArrayList("testjson");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testGetTable() {
        Table table = metadata.getTable(new ConnectContext(), "testdb", "testjson");
        Assertions.assertNotNull(table);
    }
}