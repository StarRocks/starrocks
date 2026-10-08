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
 * Where the groups of an LDAP-authenticated user come from.
 * <p>
 * Value of `authentication_ldap_simple_group_source`, settable both as a cluster-wide default
 * (FE config) and as a security integration property.
 * <p>
 * There are deliberately two entry points for parsing:
 * <ul>
 *     <li>{@link #parseOrThrow} for DDL (CREATE / ALTER SECURITY INTEGRATION), where a typo must be
 *     rejected on the spot;</li>
 *     <li>{@link #parseOrDefault} for the runtime login path, where a typo (which can also reach us
 *     through `fe.conf` / `ADMIN SET FRONTEND CONFIG`, neither of which validates enum values) must
 *     not break every LDAP login - it falls back to {@link #GROUP_PROVIDER} and logs an ERROR.</li>
 * </ul>
 */
public enum LdapGroupSource {
    /**
     * Only the configured group providers are used. This is the default, so the behavior of an
     * upgraded cluster is unchanged.
     */
    GROUP_PROVIDER("group_provider"),

    /**
     * Only the `memberOf` attribute of the user's own entry is used. Configured group providers are
     * ignored but kept, so switching back is a single ALTER.
     */
    MEMBEROF("memberof"),

    /**
     * Union of both sources.
     */
    BOTH("both");

    private static final Logger LOG = LogManager.getLogger(LdapGroupSource.class);

    private final String value;

    LdapGroupSource(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * @return true if the user's `memberOf` attribute has to be read during authentication.
     */
    public boolean readsMemberOf() {
        return this == MEMBEROF || this == BOTH;
    }

    /**
     * @return true if the configured group providers still have to be consulted.
     */
    public boolean usesGroupProvider() {
        return this == GROUP_PROVIDER || this == BOTH;
    }

    private static LdapGroupSource parse(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }
        String normalized = value.trim().toLowerCase();
        for (LdapGroupSource source : values()) {
            if (source.value.equals(normalized)) {
                return source;
            }
        }
        return null;
    }

    private static String legalValues() {
        return Arrays.stream(values()).map(LdapGroupSource::getValue).collect(Collectors.joining(", "));
    }

    /**
     * DDL path: reject an illegal value so the user gets the feedback while typing.
     */
    public static LdapGroupSource parseOrThrow(String value) {
        LdapGroupSource source = parse(value);
        if (source == null) {
            throw new SemanticException("invalid value '" + value + "' for property '"
                    + SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE
                    + "', legal values are: " + legalValues());
        }
        return source;
    }

    /**
     * Runtime path: never fail the login because of a misconfigured value, but make it visible.
     */
    public static LdapGroupSource parseOrDefault(String value) {
        if (Strings.isNullOrEmpty(value)) {
            // Not configured at all: keep the default silently.
            return GROUP_PROVIDER;
        }
        LdapGroupSource source = parse(value);
        if (source == null) {
            LOG.error("invalid value '{}' for '{}', legal values are: {}. Falling back to '{}'.",
                    value, SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE,
                    legalValues(), GROUP_PROVIDER.value);
            return GROUP_PROVIDER;
        }
        return source;
    }
}
