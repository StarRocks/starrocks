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

package com.starrocks.epack.connector.lakeformation;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationCatalogPropertiesTest {

    private static Map<String, String> validProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hive");
        properties.put("hive.metastore.type", "glue");
        properties.put("catalog.access.control", "lakeformation");
        properties.put("aws.lakeformation.session_tag_value", "starrocks");
        properties.put("aws.glue.region", "us-west-2");
        return properties;
    }

    /** In production catalogType comes from ConnectorContext.getType(); the map's "type" is a stand-in. */
    private static LakeFormationCatalogProperties parse(Map<String, String> properties) {
        return LakeFormationCatalogProperties.from(properties.get("type"), properties);
    }

    private static LakeFormationTableAccessException expectRejected(Map<String, String> properties) {
        return assertThrows(LakeFormationTableAccessException.class, () -> parse(properties));
    }

    @Test
    public void testValidPropertiesWithDefaultDuration() {
        LakeFormationCatalogProperties properties = parse(validProperties());
        assertEquals(3600, properties.credentialDurationSeconds());
        assertEquals("us-west-2", properties.region());
        assertEquals("starrocks", properties.sessionTagValue());
    }

    @Test
    public void testRequestedOnlyWhenSetOnTheCatalogItself() {
        assertTrue(LakeFormationCatalogProperties.isRequested(validProperties()));
        assertTrue(LakeFormationCatalogProperties.isRequested(
                Maps.newHashMap(Map.of("catalog.access.control", " LakeFormation "))));

        // A catalog that says nothing is not a Lake Formation catalog, even when the cluster-wide
        // Config.access_control says lakeformation - that must never enable it implicitly.
        assertFalse(LakeFormationCatalogProperties.isRequested(Maps.newHashMap()));
        assertFalse(LakeFormationCatalogProperties.isRequested(
                Maps.newHashMap(Map.of("catalog.access.control", "native"))));
        assertFalse(LakeFormationCatalogProperties.isRequested(
                Maps.newHashMap(Map.of("catalog.access.control", "ranger"))));
    }

    @Test
    public void testDurationRange() {
        Map<String, String> properties = validProperties();
        properties.put("aws.lakeformation.credential_duration_seconds", "21600");
        assertEquals(21600, parse(properties).credentialDurationSeconds());

        properties.put("aws.lakeformation.credential_duration_seconds", "899");
        assertTrue(expectRejected(properties).getMessage().contains("900"));

        properties.put("aws.lakeformation.credential_duration_seconds", "43201");
        assertTrue(expectRejected(properties).getMessage().contains("43200"));

        properties.put("aws.lakeformation.credential_duration_seconds", "1h");
        assertTrue(expectRejected(properties).getMessage().contains("1h"));
    }

    @Test
    public void testUnknownLakeFormationKeyIsRejected() {
        Map<String, String> properties = validProperties();
        properties.put("aws.lakeformation.enabled", "true");
        assertTrue(expectRejected(properties).getMessage().contains("aws.lakeformation.enabled"));
    }

    @Test
    public void testRangerIsMutuallyExclusive() {
        Map<String, String> properties = validProperties();
        properties.put("ranger.plugin.hive.service.name", "hive_service");
        assertTrue(expectRejected(properties).getMessage().toLowerCase().contains("ranger"));
    }

    /** deltalake is rejected until it has an authorized-table carrier of its own. */
    @Test
    public void testCatalogTypeAllowlist() {
        parse(validProperties());

        for (String rejected : new String[] {"deltalake", "iceberg", "hudi", "paimon", "unified", "jdbc"}) {
            Map<String, String> properties = validProperties();
            properties.put("type", rejected);
            assertTrue(expectRejected(properties).getMessage().contains(rejected));
        }
    }

    @Test
    public void testMetastoreTypeMustBeGlue() {
        Map<String, String> properties = validProperties();
        properties.put("hive.metastore.type", "hive");
        assertTrue(expectRejected(properties).getMessage().contains("glue"));
    }

    @Test
    public void testGlueRegionIsTheSingleSourceOfTruth() {
        Map<String, String> properties = validProperties();
        properties.remove("aws.glue.region");
        assertTrue(expectRejected(properties).getMessage().contains("aws.glue.region"));

        // There is no separate aws.lakeformation.region: it would be an unknown key.
        Map<String, String> withLfRegion = validProperties();
        withLfRegion.put("aws.lakeformation.region", "us-west-2");
        assertTrue(expectRejected(withLfRegion).getMessage().contains("aws.lakeformation.region"));
    }

    /** Optional, and read from the Glue property so the id cannot drift from the one the client uses. */
    @Test
    public void testCatalogIdIsOptionalAndComesFromTheGlueProperty() {
        assertNull(parse(validProperties()).awsCatalogId());

        Map<String, String> withId = validProperties();
        withId.put("aws.glue.catalog_id", " 123456789012 ");
        assertEquals("123456789012", parse(withId).awsCatalogId());

        Map<String, String> blank = validProperties();
        blank.put("aws.glue.catalog_id", "   ");
        assertNull(parse(blank).awsCatalogId());
    }

    @Test
    public void testSessionTagValueRequired() {
        Map<String, String> properties = validProperties();
        properties.remove("aws.lakeformation.session_tag_value");
        assertTrue(expectRejected(properties).getMessage().contains("session_tag_value"));
    }

    /**
     * Optional: absent means the session assumes the caller's own role. When it is given it must come
     * back verbatim, because it is the role Lake Formation authorizes - not a name we may normalize.
     */
    @Test
    public void testIamRoleArnIsOptionalAndReadBackVerbatim() {
        assertNull(parse(validProperties()).iamRoleArn());

        Map<String, String> properties = validProperties();
        properties.put("aws.lakeformation.iam_role_arn", "arn:aws:iam::123456789012:role/Engine");
        assertEquals("arn:aws:iam::123456789012:role/Engine", parse(properties).iamRoleArn());
    }
}
