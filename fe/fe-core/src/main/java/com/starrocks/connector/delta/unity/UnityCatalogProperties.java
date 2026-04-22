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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Map;

public class UnityCatalogProperties {
    public static final String UNITY_CATALOG_HOST = "unity.catalog.host";
    public static final String UNITY_CATALOG_TOKEN = "unity.catalog.token";
    public static final String UNITY_CATALOG_NAME = "unity.catalog.name";
    public static final String UNITY_VENDED_CREDENTIALS_ENABLED = "unity.catalog.vended-credentials-enabled";
    public static final String UNITY_REQUEST_TIMEOUT_MS = "unity.catalog.request-timeout-ms";
    public static final String UNITY_MAX_RETRIES = "unity.catalog.max-retries";
    // Optional S3 region that gets attached to per-table vended credentials so the BE's AWS C++
    // SDK does not default to us-east-1 and hit a 301 PermanentRedirect on buckets in other
    // regions. UC's temporary-credentials API does not return the region itself.
    public static final String UNITY_AWS_REGION = "unity.catalog.aws.region";

    private final String host;
    private final String token;
    private final String ucCatalogName;
    private final boolean vendedCredentialsEnabled;
    private final long requestTimeoutMs;
    private final int maxRetries;
    private final String awsRegion;

    public UnityCatalogProperties(Map<String, String> properties) {
        String hostValue = properties.get(UNITY_CATALOG_HOST);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hostValue),
                "%s must be set when creating a Unity Catalog-backed delta lake catalog", UNITY_CATALOG_HOST);
        this.host = stripTrailingSlash(hostValue);

        this.token = properties.get(UNITY_CATALOG_TOKEN);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.token),
                "%s must be set when creating a Unity Catalog-backed delta lake catalog", UNITY_CATALOG_TOKEN);

        this.ucCatalogName = properties.get(UNITY_CATALOG_NAME);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.ucCatalogName),
                "%s must be set when creating a Unity Catalog-backed delta lake catalog", UNITY_CATALOG_NAME);

        String vendedCredsRaw = properties.getOrDefault(UNITY_VENDED_CREDENTIALS_ENABLED, "true");
        this.vendedCredentialsEnabled = Boolean.parseBoolean(vendedCredsRaw);

        this.requestTimeoutMs = parseLong(properties, UNITY_REQUEST_TIMEOUT_MS, 30_000L);
        this.maxRetries = (int) parseLong(properties, UNITY_MAX_RETRIES, 3L);

        String region = properties.get(UNITY_AWS_REGION);
        this.awsRegion = Strings.isNullOrEmpty(region) ? null : region.trim();
    }

    public String getHost() {
        return host;
    }

    public String getToken() {
        return token;
    }

    public String getUcCatalogName() {
        return ucCatalogName;
    }

    public boolean isVendedCredentialsEnabled() {
        return vendedCredentialsEnabled;
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    /** Optional AWS region hint used when translating UC-vended S3 credentials; may be null. */
    public String getAwsRegion() {
        return awsRegion;
    }

    private static String stripTrailingSlash(String value) {
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static long parseLong(Map<String, String> properties, String key, long defaultValue) {
        String raw = properties.get(key);
        if (Strings.isNullOrEmpty(raw)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new SemanticException("Invalid numeric value for property %s: %s", key, raw);
        }
    }
}
