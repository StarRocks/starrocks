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

package com.starrocks.connector.starrocks;

import com.starrocks.connector.config.Config;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for how a StarRocks external catalog's PROPERTIES are loaded and validated: defaults,
 * required fields, and the rejection of properties this catalog does not understand.
 */
public class StarRocksConnectorConfigTest {

    /**
     * SUPPORTED_PROPERTIES is written out by hand so the production path stays a plain set lookup.
     * This guards the one hazard of that choice: adding a @Config field without listing it would
     * make CREATE EXTERNAL CATALOG reject the brand-new property. Fails the build instead.
     */
    @Test
    public void testSupportedPropertiesCoversEveryConfigField() {
        Set<String> declared = new HashSet<>();
        for (Field field : StarRocksConnectorConfig.class.getDeclaredFields()) {
            Config config = field.getAnnotation(Config.class);
            if (config != null) {
                declared.add(config.key());
            }
        }
        Assertions.assertFalse(declared.isEmpty(), "no @Config fields found — did the class change?");

        Set<String> missing = new HashSet<>(declared);
        missing.removeAll(StarRocksConnectorConfig.SUPPORTED_PROPERTIES);
        Assertions.assertTrue(missing.isEmpty(),
                "@Config properties absent from SUPPORTED_PROPERTIES: " + missing);

        // And nothing extra beyond the declared properties plus the two framework keys.
        Set<String> unexpected = new HashSet<>(StarRocksConnectorConfig.SUPPORTED_PROPERTIES);
        unexpected.removeAll(declared);
        unexpected.remove(StarRocksConnectorConfig.CATALOG_TYPE);
        unexpected.remove(StarRocksConnectorConfig.CATALOG_ACCESS_CONTROL);
        Assertions.assertTrue(unexpected.isEmpty(),
                "SUPPORTED_PROPERTIES has entries backed by neither @Config nor a framework key: "
                        + unexpected);
    }

    // A-1: http endpoint loads with brpc default and empty password
    @Test
    public void testConnectorConfigLoadsHttpEndpointDefaultsToBrpcAndEmptyPassword() {
        Map<String, String> properties = new HashMap<>();
        properties.put("starrocks.fe.http.url", "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(properties);

        Assertions.assertEquals(10000, config.getFeHttpTimeoutMs());
        Assertions.assertEquals(3, config.getFeHttpRetryTimes());
        Assertions.assertEquals("", config.getFePassword());
        Assertions.assertEquals(StarRocksConnectorConfig.TRANSPORT_BRPC_CHUNK, config.getScanTransport());
    }

    @Test
    public void testConnectorConfigLoadsCacheProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(StarRocksConnectorConfig.FE_HTTP_URL, "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(properties);
        Assertions.assertTrue(config.isCacheEnabled());
        Assertions.assertEquals(300, config.getCacheRefreshSec());
        Assertions.assertEquals(3600, config.getCacheTtlSec());
        Assertions.assertEquals(10000, config.getCacheTableMaxNum());
        Assertions.assertEquals(100000, config.getCachePartitionMaxNum());

        properties.put(StarRocksConnectorConfig.CACHE_ENABLE, "false");
        properties.put(StarRocksConnectorConfig.CACHE_REFRESH_SEC, "30");
        properties.put(StarRocksConnectorConfig.CACHE_TTL_SEC, "60");
        properties.put(StarRocksConnectorConfig.CACHE_TABLE_MAX_NUM, "5");
        properties.put(StarRocksConnectorConfig.CACHE_PARTITION_MAX_NUM, "7");
        config = new StarRocksConnectorConfig();
        config.loadConfig(properties);
        Assertions.assertFalse(config.isCacheEnabled());
        Assertions.assertEquals(30, config.getCacheRefreshSec());
        Assertions.assertEquals(60, config.getCacheTtlSec());
        Assertions.assertEquals(5, config.getCacheTableMaxNum());
        Assertions.assertEquals(7, config.getCachePartitionMaxNum());
    }

    @Test
    public void testConnectorConfigLoadsCacheRefreshThreadNum() {
        Map<String, String> properties = new HashMap<>();
        properties.put(StarRocksConnectorConfig.FE_HTTP_URL, "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(properties);
        Assertions.assertEquals(4, config.getCacheRefreshThreadNum());

        properties.put(StarRocksConnectorConfig.CACHE_REFRESH_THREAD_NUM, "2");
        config = new StarRocksConnectorConfig();
        config.loadConfig(properties);
        Assertions.assertEquals(2, config.getCacheRefreshThreadNum());
    }

    @Test
    public void testConnectorConfigRejectsInternalMarkerProperty() {
        // The __encrypted marker is appended internally when obfuscating credentials
        // for the checkpoint image; a user-supplied one would make the image loader
        // fail on restart and silently drop the catalog.
        Map<String, String> properties = new HashMap<>();
        properties.put(StarRocksConnectorConfig.FE_HTTP_URL, "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");
        properties.put(StarRocksConnectorConfig.PASSWORD + ".__encrypted", "true");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        Assertions.assertThrows(StarRocksConnectorException.class, () -> config.loadConfig(properties));
    }

    // A-2: new keys load correctly into StarRocksConnectorConfig
    @Test
    public void testConnectorConfigLoadsNewUserAndPassword() {
        Map<String, String> properties = new HashMap<>();
        properties.put("starrocks.fe.http.url", "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");
        properties.put(StarRocksConnectorConfig.PASSWORD, "secret");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(properties);

        Assertions.assertEquals("alice", config.getFeUser());
        Assertions.assertEquals("secret", config.getFePassword());
    }

    // A-3: missing user must fail instead of defaulting to root
    @Test
    public void testConnectorConfigRequiresUser() {
        Map<String, String> properties = new HashMap<>();
        properties.put("starrocks.fe.http.url", "127.0.0.1:8030");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        StarRocksConnectorException exception = Assertions.assertThrows(
                StarRocksConnectorException.class, () -> config.loadConfig(properties));

        Assertions.assertTrue(exception.getMessage().contains(StarRocksConnectorConfig.USER));
    }

    private static Map<String, String> minimalProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "starrocks");
        properties.put(StarRocksConnectorConfig.FE_HTTP_URL, "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");
        return properties;
    }

    /**
     * The base ConnectorConfig silently ignores keys it does not recognize, so a misspelled
     * property would run on the default while SHOW CREATE CATALOG still echoes it back. Every
     * unknown key must therefore fail the DDL.
     */
    @Test
    public void testConnectorConfigRejectsUnknownProperty() {
        Map<String, String> properties = minimalProperties();
        // A plausible typo: the real key is starrocks.cache.ttl.sec.
        properties.put("starrocks.cache.ttl", "60");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        StarRocksConnectorException exception = Assertions.assertThrows(
                StarRocksConnectorException.class, () -> config.loadConfig(properties));
        Assertions.assertTrue(exception.getMessage().contains("starrocks.cache.ttl"),
                exception.getMessage());
        // The message lists what is accepted so the caller can spot the typo.
        Assertions.assertTrue(exception.getMessage().contains(StarRocksConnectorConfig.CACHE_TTL_SEC),
                exception.getMessage());
    }

    /** The legacy thrift endpoint keys are covered by the same unknown-property check. */
    @Test
    public void testConnectorConfigRejectsThriftEndpointProperty() {
        Map<String, String> properties = minimalProperties();
        properties.put("starrocks.fe.thrift.url", "127.0.0.1:9020");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        StarRocksConnectorException exception = Assertions.assertThrows(
                StarRocksConnectorException.class, () -> config.loadConfig(properties));
        Assertions.assertTrue(exception.getMessage().contains("starrocks.fe.thrift.url"),
                exception.getMessage());
    }

    /**
     * ranger.plugin.hive.service.name binds the catalog to a Ranger service of type hive, whose
     * resource model has only database and table and cannot express the catalog dimension; it also
     * silently overrides catalog.access.control. catalog.access.control = ranger is the supported
     * way in.
     */
    @Test
    public void testConnectorConfigRejectsHiveRangerServiceProperty() {
        Map<String, String> properties = minimalProperties();
        properties.put("ranger.plugin.hive.service.name", "hive_service");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        StarRocksConnectorException exception = Assertions.assertThrows(
                StarRocksConnectorException.class, () -> config.loadConfig(properties));
        Assertions.assertTrue(exception.getMessage().contains("ranger.plugin.hive.service.name"),
                exception.getMessage());
    }

    /** type and catalog.access.control are framework keys and must be accepted. */
    @Test
    public void testConnectorConfigAcceptsFrameworkProperties() {
        Map<String, String> properties = minimalProperties();
        properties.put("catalog.access.control", "ranger");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        Assertions.assertDoesNotThrow(() -> config.loadConfig(properties));
    }

    /** Every declared property must pass the check when set together. */
    @Test
    public void testConnectorConfigAcceptsEveryDeclaredProperty() {
        Map<String, String> properties = minimalProperties();
        properties.put(StarRocksConnectorConfig.PASSWORD, "secret");
        properties.put(StarRocksConnectorConfig.SCAN_TRANSPORT,
                StarRocksConnectorConfig.TRANSPORT_ARROW_FLIGHT);
        properties.put(StarRocksConnectorConfig.HTTP_TIMEOUT_MS, "5000");
        properties.put(StarRocksConnectorConfig.HTTP_RETRY_TIMES, "5");
        properties.put(StarRocksConnectorConfig.CACHE_ENABLE, "false");
        properties.put(StarRocksConnectorConfig.CACHE_REFRESH_SEC, "30");
        properties.put(StarRocksConnectorConfig.CACHE_TTL_SEC, "60");
        properties.put(StarRocksConnectorConfig.CACHE_TABLE_MAX_NUM, "5");
        properties.put(StarRocksConnectorConfig.CACHE_PARTITION_MAX_NUM, "7");
        properties.put(StarRocksConnectorConfig.CACHE_REFRESH_THREAD_NUM, "2");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        Assertions.assertDoesNotThrow(() -> config.loadConfig(properties));
        Assertions.assertEquals(60, config.getCacheTtlSec());
        Assertions.assertEquals(2, config.getCacheRefreshThreadNum());
    }

    // A-5: timeout/retry property names are http-based
    @Test
    public void testConnectorConfigLoadsHttpTimeoutAndRetryProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("starrocks.fe.http.url", "127.0.0.1:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");
        properties.put("starrocks.fe.http.timeout.ms", "1234");
        properties.put("starrocks.fe.http.retry.times", "5");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(properties);

        Assertions.assertEquals(1234, config.getFeHttpTimeoutMs());
        Assertions.assertEquals(5, config.getFeHttpRetryTimes());
    }

    // A-6: http URL single-endpoint is stored and retrievable via getFeHttpUrl()
    @Test
    public void testConnectorConfigLoadsFeHttpUrlSingleEndpoint() {
        Map<String, String> properties = new HashMap<>();
        properties.put("starrocks.fe.http.url", "fe-host:8030");
        properties.put(StarRocksConnectorConfig.USER, "alice");

        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(properties);

        Assertions.assertEquals("fe-host:8030", config.getFeHttpUrl());
    }
}
