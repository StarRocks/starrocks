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
import com.starrocks.sql.analyzer.SemanticException;

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
    public static final String AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN = "authentication_ldap_simple_bind_dn_pattern";
    public static final String AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE = "authentication_ldap_simple_group_source";
    public static final String AUTHENTICATION_LDAP_SIMPLE_MEMBEROF_ATTR = "authentication_ldap_simple_memberof_attr";

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
        String ldapBindDnPattern = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                Config.authentication_ldap_simple_bind_dn_pattern);
        // Runtime path: an illegal value falls back to `group_provider` with an ERROR log instead of
        // failing the login, because the same value can also arrive from `fe.conf`, which has no
        // enum validation hook - a single typo must not lock every LDAP user out.
        GroupSource ldapGroupSource = GroupSource.parseOrDefault(
                propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE,
                        Config.authentication_ldap_simple_group_source),
                AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE);
        String ldapMemberOfAttr = propertyMap.getOrDefault(AUTHENTICATION_LDAP_SIMPLE_MEMBEROF_ATTR,
                Config.authentication_ldap_simple_memberof_attr);

        return new LDAPAuthProvider(ldapServerHost,
                ldapServerPort,
                useSSL,
                trustStorePath,
                trustStorePwd,
                ldapBindRootDn,
                ldapBindRootPwd,
                ldapBindBaseDn,
                ldapUserSearchAttr,
                null,
                ldapBindDnPattern,
                ldapGroupSource,
                ldapMemberOfAttr);
    }

    /**
     * The one value ALTER has to refuse on the spot: a misspelled group source would otherwise sit
     * in the metadata until the next login, where the runtime parse falls back to `group_provider`
     * and only logs - the administrator would see the ALTER succeed and the setting silently not
     * take effect.
     */
    @Override
    public void checkAlteredProperties(Map<String, String> alteredProperties) throws SemanticException {
        String groupSource = alteredProperties.get(AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE);
        if (groupSource != null) {
            GroupSource.parseOrThrow(groupSource, AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE);
        }
    }

    @Override
    public void checkProperty() throws SemanticException {
        super.checkProperty();
        // The base class only asserts that `type` is present, so the enum has to be validated here.
        // This covers CREATE; ALTER does not go through this hook and is validated in
        // SecurityIntegrationStatementAnalyzer.
        if (propertyMap.containsKey(AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE)) {
            GroupSource.parseOrThrow(propertyMap.get(AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE),
                    AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE);
        }
    }
}
