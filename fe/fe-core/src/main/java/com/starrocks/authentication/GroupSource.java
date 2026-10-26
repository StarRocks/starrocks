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

import com.google.common.base.Strings;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Where the groups of a login come from.
 *
 * <p>Two sources exist, and this enum says which of them take part:
 * <ul>
 *     <li>the configured <b>group providers</b> ({@code unix} / {@code file} / {@code ldap}), asked
 *     after authentication with the user name;</li>
 *     <li>what the <b>authenticator itself</b> resolved while authenticating - the {@code memberOf}
 *     attribute of the user's own LDAP entry, or a claim of the verified JWT.</li>
 * </ul>
 *
 * <p>Internally the second value is named after the <i>role</i> the source plays
 * ({@link #AUTHENTICATOR}), so one enum serves every integration type. What the administrator
 * types, though, is spelled after the mechanism of that type, because that is what tells them what
 * will actually be read: {@code memberof} on the LDAP property, {@code claim} on the token-based
 * integrations' {@code group_source}. The neutral {@code authenticator} is accepted on both. The
 * other type's spelling is refused - {@code memberof} on a JWT integration would only make a reader
 * expect a directory lookup that never happens.
 *
 * <p>Set as a security integration property, and for LDAP also as a cluster-wide default
 * ({@code authentication_ldap_simple_group_source}).
 *
 * <p>There are deliberately two entry points for parsing:
 * <ul>
 *     <li>{@link #parseOrThrow} for DDL (CREATE / ALTER SECURITY INTEGRATION), where a typo must be
 *     rejected on the spot;</li>
 *     <li>{@link #parseOrDefault} for the runtime login path, where a typo (which can also reach us
 *     through `fe.conf` / `ADMIN SET FRONTEND CONFIG`, neither of which validates enum values) must
 *     not break every login - it falls back to {@link #GROUP_PROVIDER} and logs an ERROR.</li>
 * </ul>
 */
public enum GroupSource {
    /**
     * Only the configured group providers are used. This is the default, so the behavior of an
     * upgraded cluster is unchanged.
     */
    GROUP_PROVIDER("group_provider"),
    /**
     * Only what the authenticator resolved by itself is used - LDAP `memberOf`, or a JWT claim.
     * Configured group providers are ignored but kept, so switching back is a single ALTER.
     * Spelled {@code memberof} or {@code claim} in configuration, see {@link #spelledFor}.
     */
    AUTHENTICATOR("authenticator"),
    /**
     * Union of both sources.
     */
    BOTH("both");

    /** How {@link #AUTHENTICATOR} is spelled on the LDAP property. */
    private static final String LDAP_SPELLING = "memberof";
    /** How {@link #AUTHENTICATOR} is spelled on the token-based integrations' {@code group_source}. */
    private static final String TOKEN_SPELLING = "claim";

    private static final Logger LOG = LogManager.getLogger(GroupSource.class);

    private final String value;

    GroupSource(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * @return true if the groups the authenticator resolved by itself take part in this login's
     * group set. For LDAP that means the `memberOf` attribute has to be read during authentication;
     * for JWT / OAuth2, that the configured claim has to be read out of the verified token.
     */
    public boolean usesAuthenticatorGroups() {
        return this == AUTHENTICATOR || this == BOTH;
    }

    /**
     * @return true if the configured group providers still have to be consulted.
     */
    public boolean usesGroupProvider() {
        return this == GROUP_PROVIDER || this == BOTH;
    }

    private static boolean isLdapProperty(String propertyName) {
        return SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE.equals(propertyName);
    }

    /**
     * The spelling of this value on the given property - what the administrator writes and what
     * error messages should name. Only {@link #AUTHENTICATOR} differs per property.
     */
    public String spelledFor(String propertyName) {
        if (this != AUTHENTICATOR) {
            return value;
        }
        return isLdapProperty(propertyName) ? LDAP_SPELLING : TOKEN_SPELLING;
    }

    private static GroupSource parse(String value, String propertyName) {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }
        String normalized = value.trim().toLowerCase();
        for (GroupSource source : values()) {
            if (source.value.equals(normalized) || source.spelledFor(propertyName).equals(normalized)) {
                return source;
            }
        }
        return null;
    }

    private static String legalValues(String propertyName) {
        return Arrays.stream(values()).map(s -> s.spelledFor(propertyName)).collect(Collectors.joining(", "));
    }

    /**
     * DDL path: reject an illegal value so the user gets the feedback while typing.
     *
     * @param propertyName the property being set, so the message names the one the user typed - it
     *                     is `authentication_ldap_simple_group_source` for LDAP and `group_source`
     *                     for the token-based integrations.
     */
    public static GroupSource parseOrThrow(String value, String propertyName) {
        GroupSource source = parse(value, propertyName);
        if (source == null) {
            throw new SemanticException("invalid value '" + value + "' for property '" + propertyName
                    + "', legal values are: " + legalValues(propertyName));
        }
        return source;
    }

    /**
     * Runtime path: never fail the login because of a misconfigured value, but make it visible.
     */
    public static GroupSource parseOrDefault(String value, String propertyName) {
        if (Strings.isNullOrEmpty(value)) {
            // Not configured at all: keep the default silently.
            return GROUP_PROVIDER;
        }
        GroupSource source = parse(value, propertyName);
        if (source == null) {
            LOG.error("invalid value '{}' for '{}', legal values are: {}. Falling back to '{}'.",
                    value, propertyName, legalValues(propertyName), GROUP_PROVIDER.value);
            return GROUP_PROVIDER;
        }
        return source;
    }
}
