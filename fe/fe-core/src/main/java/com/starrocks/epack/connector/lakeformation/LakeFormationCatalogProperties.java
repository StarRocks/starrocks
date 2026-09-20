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

import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.hive.HiveConnector;

import java.util.Map;
import java.util.Set;

/**
 * Parses and locally validates the Lake Formation catalog properties. Only deterministic checks
 * belong here: the account's real credential-duration ceiling cannot be probed at CREATE CATALOG
 * time, so that check happens on the first real vending and in the deployment preflight.
 *
 * One switch turns Lake Formation on - catalog.access.control=lakeformation - and it has to be set on
 * the catalog itself. A second aws.lakeformation.enabled flag would only ever produce a consistency
 * check with no meaning of its own.
 */
public class LakeFormationCatalogProperties {
    public static final String ACCESS_CONTROL = "catalog.access.control";
    public static final String ACCESS_CONTROL_LAKE_FORMATION = "lakeformation";
    public static final String RANGER_HIVE_SERVICE_NAME = "ranger.plugin.hive.service.name";
    public static final String ACCESS_CONTROL_RANGER = "ranger";
    public static final String SESSION_TAG_VALUE = "aws.lakeformation.session_tag_value";
    public static final String IAM_ROLE_ARN = "aws.lakeformation.iam_role_arn";
    public static final String CREDENTIAL_DURATION_SECONDS = "aws.lakeformation.credential_duration_seconds";

    /** Single source of truth for the region: Lake Formation and its Glue catalog are always co-located. */
    public static final String GLUE_REGION = "aws.glue.region";

    /**
     * Optional; absent means the account's own catalog. Read from the same property the Glue client
     * uses so the id in a table identity, the id in a request and the id in an error message cannot
     * drift apart - an error naming an account that was never consulted is worse than no id at all.
     */
    public static final String GLUE_CATALOG_ID = "aws.glue.catalog_id";

    private static final String LF_PROPERTY_PREFIX = "aws.lakeformation.";

    public static final int MIN_CREDENTIAL_DURATION_SECONDS = 900;
    public static final int MAX_CREDENTIAL_DURATION_SECONDS = 43200;
    public static final int DEFAULT_CREDENTIAL_DURATION_SECONDS = 3600;

    /**
     * Hive only. A Delta catalog would need its own authorized-table carrier - DeltaLakeTable extends
     * Table directly and holds a snapshot, an engine and a metastore table - so accepting deltalake
     * here would create a catalog that passes validation but has nothing to enforce column access with.
     */
    private static final Set<String> SUPPORTED_CATALOG_TYPES = ImmutableSet.of("hive");
    private static final Set<String> KNOWN_LF_PROPERTIES = ImmutableSet.of(
            SESSION_TAG_VALUE, IAM_ROLE_ARN, CREDENTIAL_DURATION_SECONDS);

    private final String region;
    private final String awsCatalogId;
    private final String sessionTagValue;
    private final String iamRoleArn;
    private final int credentialDurationSeconds;

    private LakeFormationCatalogProperties(String region, String awsCatalogId, String sessionTagValue,
                                           String iamRoleArn, int credentialDurationSeconds) {
        this.region = region;
        this.awsCatalogId = awsCatalogId;
        this.sessionTagValue = sessionTagValue;
        this.iamRoleArn = iamRoleArn;
        this.credentialDurationSeconds = credentialDurationSeconds;
    }

    public String region() {
        return region;
    }

    /** Null when the catalog did not name one, meaning the caller's own AWS account. */
    public String awsCatalogId() {
        return awsCatalogId;
    }

    public String sessionTagValue() {
        return sessionTagValue;
    }

    public String iamRoleArn() {
        return iamRoleArn;
    }

    public int credentialDurationSeconds() {
        return credentialDurationSeconds;
    }

    /**
     * True only when this catalog itself asks for Lake Formation.
     *
     * Reads the property map directly rather than the value LazyConnector resolves, because that one
     * falls back to the global Config.access_control - and a cluster-wide default must not silently
     * turn Lake Formation on for every catalog.
     */
    public static boolean isRequested(Map<String, String> properties) {
        String accessControl = properties.get(ACCESS_CONTROL);
        return accessControl != null && ACCESS_CONTROL_LAKE_FORMATION.equalsIgnoreCase(accessControl.trim());
    }

    /**
     * catalogType is the authority from ConnectorContext.getType(); this class never reads a "type"
     * key out of the property map.
     */
    public static LakeFormationCatalogProperties from(String catalogType, Map<String, String> properties) {
        rejectUnknownLakeFormationKeys(properties);
        requireNoRanger(properties);
        requireSupportedCatalogType(catalogType);
        requireGlueMetastore(properties);
        String region = requireNonEmpty(properties, GLUE_REGION);
        String sessionTagValue = requireNonEmpty(properties, SESSION_TAG_VALUE);
        String awsCatalogId = trimToNull(properties.get(GLUE_CATALOG_ID));
        return new LakeFormationCatalogProperties(region, awsCatalogId, sessionTagValue,
                trimToNull(properties.get(IAM_ROLE_ARN)), parseDuration(properties));
    }

    /**
     * A mistyped value fails fast on its own; without this a mistyped *key* would be silently ignored
     * instead, which is the same bug one level up.
     */
    private static void rejectUnknownLakeFormationKeys(Map<String, String> properties) {
        for (String key : properties.keySet()) {
            if (key.startsWith(LF_PROPERTY_PREFIX) && !KNOWN_LF_PROPERTIES.contains(key)) {
                throw reject("unknown property '" + key + "'; expected one of " + KNOWN_LF_PROPERTIES);
            }
        }
    }

    /**
     * Checked only because this catalog asked for Lake Formation - a plain Ranger catalog never reaches
     * here. A non-empty ranger service name overrides catalog.access.control entirely, so it has to be
     * rejected rather than quietly winning.
     */
    private static void requireNoRanger(Map<String, String> properties) {
        String serviceName = properties.get(RANGER_HIVE_SERVICE_NAME);
        if (serviceName != null && !serviceName.trim().isEmpty()) {
            throw reject("Ranger and Lake Formation are mutually exclusive, but "
                    + RANGER_HIVE_SERVICE_NAME + "=" + serviceName + " is set");
        }
    }

    private static void requireSupportedCatalogType(String catalogType) {
        String normalized = catalogType == null ? "" : catalogType.trim().toLowerCase();
        if (!SUPPORTED_CATALOG_TYPES.contains(normalized)) {
            throw reject("Lake Formation supports catalog types " + SUPPORTED_CATALOG_TYPES
                    + ", but the catalog type is " + catalogType);
        }
    }

    /** Reuses HiveConnector.HIVE_METASTORE_TYPE rather than redeclaring the key. */
    private static void requireGlueMetastore(Map<String, String> properties) {
        String metastoreType = properties.get(HiveConnector.HIVE_METASTORE_TYPE);
        if (metastoreType == null || !"glue".equalsIgnoreCase(metastoreType.trim())) {
            throw reject("Lake Formation requires " + HiveConnector.HIVE_METASTORE_TYPE
                    + "=glue, but it is " + metastoreType);
        }
    }

    private static int parseDuration(Map<String, String> properties) {
        String raw = properties.get(CREDENTIAL_DURATION_SECONDS);
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_CREDENTIAL_DURATION_SECONDS;
        }
        int seconds;
        try {
            seconds = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw reject(CREDENTIAL_DURATION_SECONDS + " must be an integer number of seconds, got '" + raw + "'");
        }
        if (seconds < MIN_CREDENTIAL_DURATION_SECONDS || seconds > MAX_CREDENTIAL_DURATION_SECONDS) {
            throw reject(CREDENTIAL_DURATION_SECONDS + "=" + seconds + " is out of range ["
                    + MIN_CREDENTIAL_DURATION_SECONDS + ", " + MAX_CREDENTIAL_DURATION_SECONDS + "]");
        }
        return seconds;
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static String requireNonEmpty(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw reject(key + " is required");
        }
        return value.trim();
    }

    private static LakeFormationTableAccessException reject(String reason) {
        return new LakeFormationTableAccessException("Invalid Lake Formation catalog configuration: " + reason);
    }
}
