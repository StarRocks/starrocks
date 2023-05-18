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

import java.util.HashMap;
import java.util.Map;

/**
 * Security integration specified in `Config.authentication_chain`.
 * Authentication for this integration is provided by member `authenticationProvider`.
 */
public class LDAPSecurityIntegration extends SecurityIntegration {
    public static final String LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY = "ldap_bind_base_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY = "ldap_bind_root_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY = "ldap_bind_root_pwd";
    public static final String LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY = "ldap_cache_refresh_interval";


    public LDAPSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
        return AuthenticationProviderFactory.create(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name());
    }

    public String getLdapServerHost() {
        return propertyMap.getOrDefault("ldap_server_host", "127.0.0.1");
    }

    public String getLdapServerPort() {
        return propertyMap.getOrDefault("ldap_server_port", "389");
    }

    public String getLdapBindBaseDn() {
        return propertyMap.get(LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY);
    }

    public String getLdapUserSearchAttr() {
        return propertyMap.getOrDefault("ldap_user_search_attr", "uid");
    }

    public String getLdapUserGroupMatchAttr() {
        return propertyMap.getOrDefault("ldap_user_group_match_attr", getLdapUserSearchAttr());
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
    public String toString() {
        return "name: " + name + ", properties: " + getPropertyMapWithMasking();
    }
}
