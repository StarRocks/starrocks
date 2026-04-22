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

package com.starrocks.connector.delta.unity;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class UnityCatalogPropertiesTest {

    @Test
    public void testBasicProperties() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com/",
                "unity.catalog.token", "dapiXYZ",
                "unity.catalog.name", "main");
        UnityCatalogProperties p = new UnityCatalogProperties(props);
        Assertions.assertEquals("https://example.cloud.databricks.com", p.getHost());
        Assertions.assertEquals("dapiXYZ", p.getToken());
        Assertions.assertEquals("main", p.getUcCatalogName());
        Assertions.assertTrue(p.isVendedCredentialsEnabled(), "vended creds should default to true");
        Assertions.assertEquals(30_000L, p.getRequestTimeoutMs());
        Assertions.assertEquals(3, p.getMaxRetries());
        Assertions.assertNull(p.getAwsRegion(), "aws region should default to null");
    }

    @Test
    public void testAwsRegionParsed() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiXYZ",
                "unity.catalog.name", "main",
                "unity.catalog.aws.region", "  eu-central-1  ");
        UnityCatalogProperties p = new UnityCatalogProperties(props);
        Assertions.assertEquals("eu-central-1", p.getAwsRegion());
    }

    @Test
    public void testBlankAwsRegionTreatedAsNull() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiXYZ",
                "unity.catalog.name", "main",
                "unity.catalog.aws.region", "");
        UnityCatalogProperties p = new UnityCatalogProperties(props);
        Assertions.assertNull(p.getAwsRegion());
    }

    @Test
    public void testVendedCredentialsExplicitlyDisabled() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiXYZ",
                "unity.catalog.name", "main",
                "unity.catalog.vended-credentials-enabled", "false");
        UnityCatalogProperties p = new UnityCatalogProperties(props);
        Assertions.assertFalse(p.isVendedCredentialsEnabled());
    }

    @Test
    public void testCustomTimeoutAndRetries() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiXYZ",
                "unity.catalog.name", "main",
                "unity.catalog.request-timeout-ms", "9000",
                "unity.catalog.max-retries", "5");
        UnityCatalogProperties p = new UnityCatalogProperties(props);
        Assertions.assertEquals(9_000L, p.getRequestTimeoutMs());
        Assertions.assertEquals(5, p.getMaxRetries());
    }

    @Test
    public void testMissingHostFails() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.token", "dapiXYZ",
                "unity.catalog.name", "main");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new UnityCatalogProperties(props));
    }

    @Test
    public void testMissingTokenFails() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.name", "main");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new UnityCatalogProperties(props));
    }

    @Test
    public void testMissingCatalogNameFails() {
        Map<String, String> props = ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiXYZ");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new UnityCatalogProperties(props));
    }

    @Test
    public void testInvalidNumericFails() {
        Map<String, String> props = new HashMap<>();
        props.put("unity.catalog.host", "https://example.cloud.databricks.com");
        props.put("unity.catalog.token", "dapiXYZ");
        props.put("unity.catalog.name", "main");
        props.put("unity.catalog.request-timeout-ms", "not-a-number");
        Assertions.assertThrows(SemanticException.class, () -> new UnityCatalogProperties(props));
    }
}
