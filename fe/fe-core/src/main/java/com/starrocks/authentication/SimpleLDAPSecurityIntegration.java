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

import com.starrocks.common.Config;

import java.util.Map;

public class SimpleLDAPSecurityIntegration extends SecurityIntegration {
    public static final String AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST = "authentication_ldap_simple_server_host";
    public static final String AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT = "authentication_ldap_simple_server_port";
    public static final String AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_ALLOW_INSECURE
            = "authentication_ldap_simple_ssl_conn_allow_insecure";
    public static final String AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_TRUST_STORE_PATH
            = "authentication_ldap_simple_ssl_conn_trust_store_path";
    public static final String AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_TRUST_STORE_PWD
            = "authentication_ldap_simple_ssl_conn_trust_store_pwd";
    public static final String AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN = "authentication_ldap_simple_bind_root_dn";
    public static final String AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD = "authentication_ldap_simple_bind_root_pwd";
    public static final String AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN = "authentication_ldap_simple_bind_base_dn";
    public static final String AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR = "authentication_ldap_simple_user_search_attr";

    public SimpleLDAPSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
        String ldapServerHost = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST,
                Config.authentication_ldap_simple_server_host);
        int ldapServerPort = propertyMap.containsKey(AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT) ?
                Integer.parseInt(propertyMap.get(AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT)) :
                Config.authentication_ldap_simple_server_port;
        boolean useSSL = !(propertyMap.containsKey(AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_ALLOW_INSECURE) ?
                Boolean.parseBoolean(propertyMap.get(AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_ALLOW_INSECURE)) :
                Config.authentication_ldap_simple_ssl_conn_allow_insecure);
        String trustStorePath = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_TRUST_STORE_PATH,
                Config.authentication_ldap_simple_ssl_conn_trust_store_path);
        String trustStorePwd = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_SSL_CONN_TRUST_STORE_PWD,
                Config.authentication_ldap_simple_ssl_conn_trust_store_pwd);
        String ldapBindRootDn = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN,
                Config.authentication_ldap_simple_bind_root_dn);
        String ldapBindRootPwd = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD,
                Config.authentication_ldap_simple_bind_root_pwd);
        String ldapBindBaseDn = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN,
                Config.authentication_ldap_simple_bind_base_dn);
        String ldapUserSearchAttr = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR,
                Config.authentication_ldap_simple_user_search_attr);

        return new LDAPAuthProvider(ldapServerHost,
                ldapServerPort,
                useSSL,
                trustStorePath,
                trustStorePwd,
                ldapBindRootDn,
                ldapBindRootPwd,
                ldapBindBaseDn,
                ldapUserSearchAttr,
                null);
    }
}
