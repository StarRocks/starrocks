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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.mysql.privilege.AuthPlugin;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders the current state of a user as a replayable CREATE USER statement, for SHOW CREATE USER.
 * <p>
 * Everything is read from live state ({@link UserAuthenticationInfo}, the user's default roles and
 * {@link UserProperty}), so the output reflects any ALTER USER / SET PASSWORD applied since creation.
 * When {@code showSecret} is false the password digest and the auth string are replaced by
 * {@link #SECRET_PLACEHOLDER}; every other clause is rendered verbatim.
 * <p>
 * Values rendered into single-quoted literals go through {@link SqlUtils#escapeSqlString}. The
 * PROPERTIES clause is rendered by {@link PrintableMap}, which escapes only the double quote, so a
 * property value containing a backslash is not escaped here -- a pre-existing limitation shared by
 * every PROPERTIES renderer in the codebase.
 */
public class CreateUserDdlBuilder {
    public static final String SECRET_PLACEHOLDER = "<secret>";

    private CreateUserDdlBuilder() {
    }

    public static String build(UserIdentity userIdentity,
                               UserAuthenticationInfo authenticationInfo,
                               List<String> defaultRoles,
                               UserProperty userProperty,
                               boolean showSecret) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE USER ").append(renderUserIdentity(userIdentity));

        appendAuthOption(sb, authenticationInfo, showSecret);

        if (defaultRoles != null && !defaultRoles.isEmpty()) {
            // getDefaultRoleIdsByUser returns an unordered Set, so sort for a stable rendering
            sb.append("\nDEFAULT ROLE ").append(defaultRoles.stream()
                    .sorted()
                    .map(role -> "'" + SqlUtils.escapeSqlString(role) + "'")
                    .collect(Collectors.joining(", ")));
        }

        if (authenticationInfo.isPasswordExpired()) {
            sb.append("\nEXPIRE_PASSWORD = true");
        }

        if (authenticationInfo.isLock()) {
            sb.append("\nLOCK");
        }

        Map<String, String> properties = collectNonDefaultProperties(userProperty);
        if (!properties.isEmpty()) {
            sb.append("\nPROPERTIES (")
                    .append(new PrintableMap<>(properties, "=", true, false))
                    .append(")");
        }

        return sb.toString();
    }

    /**
     * Mirrors {@link UserIdentity#toString()} but escapes each part, so that a host pattern
     * containing a quote still yields a parseable statement. Host patterns are only validated by
     * compiling them as a MySQL pattern, so they can legitimately carry quotes.
     */
    private static String renderUserIdentity(UserIdentity userIdentity) {
        StringBuilder sb = new StringBuilder();
        sb.append("'").append(SqlUtils.escapeSqlString(userIdentity.getUser())).append("'@");

        String host = userIdentity.getHost();
        if (Strings.isNullOrEmpty(host)) {
            sb.append("%");
        } else if (userIdentity.isDomain()) {
            sb.append("['").append(SqlUtils.escapeSqlString(host)).append("']");
        } else {
            sb.append("'").append(SqlUtils.escapeSqlString(host)).append("'");
        }

        return sb.toString();
    }

    private static void appendAuthOption(StringBuilder sb, UserAuthenticationInfo authenticationInfo,
                                         boolean showSecret) {
        String authPlugin = authenticationInfo.getAuthPlugin();
        if (Strings.isNullOrEmpty(authPlugin)) {
            return;
        }

        if (AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString().equalsIgnoreCase(authPlugin)) {
            byte[] password = authenticationInfo.getPassword();
            if (password == null || password.length == 0) {
                return;
            }
            sb.append("\nIDENTIFIED WITH ").append(authPlugin.toLowerCase(Locale.ROOT)).append(" AS '")
                    .append(showSecret ? new String(password, StandardCharsets.UTF_8) : SECRET_PLACEHOLDER)
                    .append("'");
            return;
        }

        sb.append("\nIDENTIFIED WITH ").append(authPlugin.toLowerCase(Locale.ROOT));
        String authString = authenticationInfo.getAuthString();
        if (!Strings.isNullOrEmpty(authString)) {
            sb.append(" AS '")
                    .append(showSecret ? SqlUtils.escapeSqlString(authString) : SECRET_PLACEHOLDER)
                    .append("'");
        }
    }

    private static Map<String, String> collectNonDefaultProperties(UserProperty userProperty) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (userProperty == null) {
            return properties;
        }

        if (userProperty.getMaxConn() != UserProperty.MAX_CONN_DEFAULT_VALUE) {
            properties.put(UserProperty.PROP_MAX_USER_CONNECTIONS, String.valueOf(userProperty.getMaxConn()));
        }
        if (!UserProperty.CATALOG_DEFAULT_VALUE.equalsIgnoreCase(userProperty.getCatalog())) {
            properties.put(UserProperty.PROP_CATALOG, userProperty.getCatalog());
        }
        if (!Strings.isNullOrEmpty(userProperty.getDatabase())) {
            properties.put(UserProperty.PROP_DATABASE, userProperty.getDatabase());
        }
        if (!Strings.isNullOrEmpty(userProperty.getPasswordPolicy())) {
            properties.put(UserProperty.PROP_PASSWORD_POLICY, userProperty.getPasswordPolicy());
        }
        for (Map.Entry<String, String> entry : userProperty.getSessionVariables().entrySet()) {
            properties.put(UserProperty.PROP_SESSION_PREFIX + entry.getKey(), entry.getValue());
        }

        return properties;
    }
}
