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

import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Security integration specified in `Config.authentication_chain`.
 * Authentication for this integration is provided by member `authenticationProvider`.
 */
public class LDAPSecurityIntegration extends SecurityIntegration {
    public static final String LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY = "ldap_bind_base_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY = "ldap_bind_root_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY = "ldap_bind_root_pwd";
    public static final String LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY = "ldap_cache_refresh_interval";

    public static final String LDAP_SERVER_HOST = "ldap_server_host";

    public static final String LDAP_SERVER_PORT = "ldap_server_port";

    public static final String LDAP_USER_SEARCH_ATTR = "ldap_user_search_attr";

    public static final String LDAP_USER_GROUP_MATCH_ATTR = "ldap_user_group_match_attr";

    private static final Set<String> REQUIRED_PROPERTIES = new HashSet<>(Arrays.asList(
            LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY,
            LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY,
            LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY));

    public LDAPSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
        return AuthenticationProviderFactory.create(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name());
    }

    public String getLdapServerHost() {
        return propertyMap.getOrDefault(LDAP_SERVER_HOST, "127.0.0.1");
    }

    public String getLdapServerPort() {
        return propertyMap.getOrDefault(LDAP_SERVER_PORT, "389");
    }

    public String getLdapBindBaseDn() {
        return propertyMap.get(LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY);
    }

    public String getLdapUserSearchAttr() {
        return propertyMap.getOrDefault(LDAP_USER_SEARCH_ATTR, "uid");
    }

    public String getLdapUserGroupMatchAttr() {
        return propertyMap.getOrDefault(LDAP_USER_GROUP_MATCH_ATTR, getLdapUserSearchAttr());
    }

    public String getLdapBindRootDn() {
        return propertyMap.get(LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY);
    }

    public String getLdapBindRootPwd() {
        return propertyMap.get(LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY);
    }

    public int getLdapCacheRefreshInterval() {
        return Integer.parseInt(
                propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY, "900"));
    }

    @Override
    public Map<String, String> getPropertyMapWithMasking() {
        Map<String, String> maskedMap = new HashMap<>(propertyMap);
        maskedMap.put(LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY, "******");

        return maskedMap;
    }

    @Override
    public void checkProperties() {
        REQUIRED_PROPERTIES.forEach(s -> {
            if (!propertyMap.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });

        if (propertyMap.containsKey(LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY)) {
            try {
                String val = propertyMap.get(LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY);
                int interval = Integer.parseInt(val);
                if (interval < 10) {
                    throw new NumberFormatException("current value of '" +
                            LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY + "' is less than 10");
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("invalid '" +
                        LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY +
                        "' property value, error: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public String toString() {
        return "name: " + name + ", properties: " + getPropertyMapWithMasking();
    }
}
