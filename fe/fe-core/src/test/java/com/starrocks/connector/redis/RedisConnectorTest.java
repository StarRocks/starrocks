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

import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class RedisConnectorTest {

    @Test
    public void testCreateRedisConnector() {
        Map<String, String> properties = new HashMap<>();

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new RedisConnector(new ConnectorContext("redis_catalog", "redis", properties)),
                "Missing redis_uri in properties");

        properties.put("redis_uri", "localhost:6379");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new RedisConnector(new ConnectorContext("redis_catalog", "redis", properties)),
                "Missing password in properties");

        properties.put("password", "xxxx");

        new RedisConnector(new ConnectorContext("reids_catalog", "redis", properties));
    }

    @Test
    public void testGetMetadata() {
        Map<String, String> properties = new HashMap<>();
        properties.put("redis_uri", "localhost:7051");
        properties.put("password", "xxxx");
        properties.put("redis.table-description-dir", "/path/redis");
        RedisConnector connector = new RedisConnector(new ConnectorContext("redis_catalog", "redis", properties));

        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof RedisMetadata);
    }

}
