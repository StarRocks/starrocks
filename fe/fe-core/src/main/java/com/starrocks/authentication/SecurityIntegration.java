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

package com.starrocks.authentication;

import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Security integration specified in `Config.authentication_chain`.
 * Authentication for this integration is provided by corresponding `getAuthenticationProvider()`.
 */
public abstract class SecurityIntegration {
    public static final String AUTHENTICATION_CHAIN_MECHANISM_NATIVE = "native";
    public static final String SECURITY_INTEGRATION_PROPERTY_TYPE_KEY = "type";
    public static final String SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER = "group_provider";
    public static final String SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN = "permitted_groups";

    @SerializedName(value = "n")
    protected String name;
    /**
     * Properties describe this integration.
     */
    @SerializedName(value = "m")
    protected Map<String, String> propertyMap;

    SecurityIntegration(String name, Map<String, String> propertyMap) {
        this.name = name;
        this.propertyMap = propertyMap;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return propertyMap.getOrDefault("comment", "");
    }

    public String getType() {
        return propertyMap.get(SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
    }

    public abstract AuthenticationProvider getAuthenticationProvider() throws AuthenticationException;

    /**
     * The issuers (iss) this integration accepts, used to route an HTTP/Arrow Flight Bearer token
     * to the right integration. Only token-based integrations (JWT / OAuth2) participate; the
     * default empty array means "never matched by Bearer routing" (e.g. LDAP).
     */
    public String[] getRequiredIssuer() {
        return new String[0];
    }

    public Map<String, String> getPropertyMapWithMasking() {
        return propertyMap;
    }

    public void checkProperty() throws SemanticException {
        if (!propertyMap.containsKey(SECURITY_INTEGRATION_PROPERTY_TYPE_KEY)) {
            throw new SemanticException("missing required property: " + SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
        }
    }

    /**
     * Validate the properties an {@code ALTER SECURITY INTEGRATION} sets, before they are merged
     * into this integration's. ALTER does not go through {@link #checkProperty()}, and must not:
     * only what the statement mentions is checked, so an integration created before a validation
     * existed can still be altered. Types with no per-property rules keep this no-op.
     */
    public void checkAlteredProperties(Map<String, String> alteredProperties) throws SemanticException {
    }

    public List<String> getGroupProviderName() {
        return splitCommaSeparated(propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER));
    }

    public List<String> getGroupAllowedLoginList() {
        return splitCommaSeparated(propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN));
    }

    /**
     * Split a comma-separated property value. Administrators write these by hand in SQL, so tolerate
     * whitespace anywhere around an item ("a , b") and drop empty items ("a,,b"). Splitting on ",\\s*"
     * only removed whitespace that followed a comma, which left a trailing blank on items such as "a "
     * and made them compare unequal to everything.
     */
    protected static List<String> splitCommaSeparated(String property) {
        if (property == null || property.isBlank()) {
            return List.of();
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .collect(Collectors.toUnmodifiableList());
    }
}
