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

import com.google.re2j.Pattern;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.HashMap;
import java.util.Map;

/**
 * What the two integration types that verify a JWT have in common: the properties that describe the
 * token (where the keys are, which claim names the user, which issuers and audiences are accepted)
 * and, since groups can be read from the verified token, the pair that says whether and from which
 * claim.
 *
 * <p>{@link JWTSecurityIntegration} adds nothing but its provider; {@link OAuth2SecurityIntegration}
 * adds the authorization-code endpoints and falls back to the cluster-wide {@code oauth2_*}
 * defaults for the properties here ({@link #configDefault}).
 */
public abstract class JWTBaseSecurityIntegration extends SecurityIntegration {
    // The names the providers already publish; an integration builds its provider, never the other
    // way round, so the dependency points this way.
    public static final String JWKS_URL = JWTAuthenticationProvider.JWT_JWKS_URL;
    public static final String PRINCIPAL_FIELD = JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD;
    public static final String REQUIRED_ISSUER = JWTAuthenticationProvider.JWT_REQUIRED_ISSUER;
    public static final String REQUIRED_AUDIENCE = JWTAuthenticationProvider.JWT_REQUIRED_AUDIENCE;
    /**
     * Where the groups of a login come from: the group providers, the token, or both. Same three
     * meanings as the LDAP property, spelled {@code claim} here; see {@link GroupSource}.
     */
    public static final String GROUP_SOURCE = "group_source";
    /**
     * Claim the groups are read from, dot-separated for nested claims. Mandatory when
     * {@link #GROUP_SOURCE} reads them off the token; there is deliberately no default, because
     * the claim an identity provider emits differs per provider and a wrong guess fails silently as
     * "logged in but has no privileges".
     */
    public static final String GROUP_CLAIM = "group_claim";

    private static final Pattern COMMA_SPLIT = Pattern.compile("\\s*,\\s*");

    protected JWTBaseSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    /**
     * The cluster-wide default for a property that is not set on this integration, or null when
     * the type has none.
     */
    protected String configDefault(String key) {
        return null;
    }

    protected String property(String key) {
        return propertyMap.getOrDefault(key, configDefault(key));
    }

    protected String getJwksUrl() {
        return property(JWKS_URL);
    }

    protected String getPrincipalField() {
        return property(PRINCIPAL_FIELD);
    }

    @Override
    public String[] getRequiredIssuer() {
        return splitList(property(REQUIRED_ISSUER));
    }

    public String[] getRequiredAudience() {
        return splitList(property(REQUIRED_AUDIENCE));
    }

    private static String[] splitList(String commaSeparated) {
        return commaSeparated == null ? new String[0] : COMMA_SPLIT.split(commaSeparated.trim());
    }

    /**
     * Runtime path: a bad value must not fail every login of this integration.
     */
    public GroupSource getGroupSource() {
        return GroupSource.parseOrDefault(propertyMap.get(GROUP_SOURCE), GROUP_SOURCE);
    }

    public String getGroupClaim() {
        return propertyMap.get(GROUP_CLAIM);
    }

    @Override
    public void checkProperty() throws SemanticException {
        super.checkProperty();
        checkGroupSourceProperties(propertyMap);
    }

    /**
     * The pair depends on each other and an ALTER may set either one alone - turning the source on
     * while the claim is already configured is legal, and so is changing the claim while the source
     * stays - so it is checked against the merged properties, which is what ALTER stores.
     */
    @Override
    public void checkAlteredProperties(Map<String, String> alteredProperties) throws SemanticException {
        if (!alteredProperties.containsKey(GROUP_SOURCE) && !alteredProperties.containsKey(GROUP_CLAIM)) {
            return;
        }
        Map<String, String> merged = new HashMap<>(propertyMap);
        merged.putAll(alteredProperties);
        checkGroupSourceProperties(merged);
    }

    /**
     * Two combinations are rejected rather than accepted and left to fail at login time:
     * <ul>
     *     <li>reading groups off the token without saying which claim - the login would succeed
     *     with no groups at all, which surfaces as "authenticated but no privileges";</li>
     *     <li>the same, together with {@code permitted_groups} - an empty group set can never
     *     intersect the allowed list, so <b>every</b> user would be refused.</li>
     * </ul>
     */
    private static void checkGroupSourceProperties(Map<String, String> propertyMap) throws SemanticException {
        String rawGroupSource = propertyMap.get(GROUP_SOURCE);
        if (rawGroupSource == null) {
            if (propertyMap.containsKey(GROUP_CLAIM)) {
                throw new SemanticException("property '" + GROUP_CLAIM + "' has no effect unless '" + GROUP_SOURCE
                        + "' is set to '" + GroupSource.AUTHENTICATOR.spelledFor(GROUP_SOURCE) + "' or '"
                        + GroupSource.BOTH.getValue() + "'");
            }
            return;
        }
        GroupSource groupSource = GroupSource.parseOrThrow(rawGroupSource, GROUP_SOURCE);
        if (!groupSource.usesAuthenticatorGroups()) {
            return;
        }
        String groupClaim = propertyMap.get(GROUP_CLAIM);
        if (groupClaim == null || groupClaim.isBlank()) {
            String because = splitCommaSeparated(propertyMap.get(SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN)).isEmpty()
                    ? "every login would resolve no group at all"
                    : "every login would resolve no group at all, and with '"
                            + SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN + "' configured that refuses every user";
            throw new SemanticException("property '" + GROUP_CLAIM + "' is required when '" + GROUP_SOURCE + "' is '"
                    + groupSource.spelledFor(GROUP_SOURCE) + "': " + because);
        }
    }
}
