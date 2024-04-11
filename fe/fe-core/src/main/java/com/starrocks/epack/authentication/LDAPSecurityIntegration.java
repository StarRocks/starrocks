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

package com.starrocks.epack.authentication;

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.epack.security.SslUtils;
import com.starrocks.mysql.privilege.AuthPlugin;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.net.ssl.SSLContext;

/**
 * Security integration specified in `Config.authentication_chain`.
 * Authentication for this integration is provided by member `authenticationProvider`.
 */
public class LDAPSecurityIntegration extends SecurityIntegration {
    private static final Logger LOG = LogManager.getLogger(LDAPSecurityIntegration.class);

    public static final String LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY = "ldap_bind_base_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY = "ldap_bind_root_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY = "ldap_bind_root_pwd";
    public static final String LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY = "ldap_cache_refresh_interval";
    /**
     * When `ldap_group_match_use_member_uid` set to "false",
     * we will not retrieve the member of the group based on `memberUid` attribute.
     */
    public static final String LDAP_SEC_INTEGRATION_PROP_USE_MEMBER_UID_KEY = "ldap_group_match_use_member_uid";
    public static final String LDAP_SEC_INTEGRATION_PROP_SSL_CONN_ALLOW_INSECURE = "ldap_ssl_conn_allow_insecure";
    public static final String LDAP_SEC_INTEGRATION_PROP_SSL_CONN_TRUST_STORE_PWD = "ldap_ssl_conn_trust_store_pwd";
    public static final String LDAP_SEC_INTEGRATION_PROP_CONN_TIMEOUT_MS_KEY = "ldap_conn_timeout";
    public static final String LDAP_SEC_INTEGRATION_PROP_CONN_READ_TIMEOUT_MS_KEY = "ldap_conn_read_timeout";

    public static final String LDAP_SEC_INTEGRATION_GROUP_MATCH_ATTR_KEY = "ldap_user_group_match_attr";

    public static final String LDAP_USER_SEARCH_DEFAULT_ATTR = "uid";

    private static final String LDAPS_SERVER_SSL_DEFAULT_PORT = "636";


    /**
     * last refresh time of group membership for all role
     * mappings connected with this security integration.
     */
    private long lastRefreshTime = -1;


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

    public String getLdapConnUrl() {
        return propertyMap.getOrDefault("ldap_conn_url", "");
    }

    public boolean isLdapSslConnAllowInsecure() {
        return Boolean.parseBoolean(
                propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_SSL_CONN_ALLOW_INSECURE, "true"));
    }

    public String getLdapSslConnTrustStorePath() {
        return propertyMap.getOrDefault("ldap_ssl_conn_trust_store_path", "");
    }

    public String getLdapSslConnTrustStorePwd() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_SSL_CONN_TRUST_STORE_PWD, "");
    }

    public String getLdapConnTimeout() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_CONN_TIMEOUT_MS_KEY, "30000");
    }

    public String getLdapConnReadTimeout() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_CONN_READ_TIMEOUT_MS_KEY, "30000");
    }

    public String getLdapBindBaseDn() {
        return propertyMap.get(LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY);
    }

    public String getLdapUserSearchAttr() {
        return propertyMap.getOrDefault("ldap_user_search_attr", LDAP_USER_SEARCH_DEFAULT_ATTR);
    }

    public String getLdapUserGroupMatchAttr() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_GROUP_MATCH_ATTR_KEY, getLdapUserSearchAttr());
    }

    public boolean getLdapGroupMatchUseMemberUid() {
        return Boolean.parseBoolean(
                propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_USE_MEMBER_UID_KEY, "true"));
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
        maskedMap.put(LDAP_SEC_INTEGRATION_PROP_SSL_CONN_TRUST_STORE_PWD, "******");

        return maskedMap;
    }

    public long getLastRefreshTime() {
        return lastRefreshTime;
    }

    public void setLastRefreshTime(long lastRefreshTime) {
        this.lastRefreshTime = lastRefreshTime;
    }

    @Override
    public String toString() {
        return "name: " + name + ", properties: " + getPropertyMapWithMasking();
    }

    public String getLdapUrlOnConnection() {
        if (getLdapConnUrl().isEmpty()) {
            String scheme = "ldap://";
            if (getLdapServerPort().equals(LDAPS_SERVER_SSL_DEFAULT_PORT)) {
                scheme = "ldaps://";
            }
            return scheme + getLdapServerHost() + ":" + getLdapServerPort();
        } else {
            return getLdapConnUrl();
        }
    }

    public DirContext createDirContextOnConnection() throws GeneralSecurityException, NamingException, IOException {
        return createDirContextOnConnection(getLdapBindRootDn(), getLdapBindRootPwd());
    }

    public DirContext createDirContextOnConnection(String dn, String pwd)
            throws GeneralSecurityException, IOException, NamingException {
        if (Strings.isNullOrEmpty(pwd)) {
            LOG.warn("empty password is not allowed for simple authentication");
            return null;
        }

        // 1. Build env
        // 1.1. Init basic env.
        String url = getLdapUrlOnConnection();
        Hashtable<String, String> environment = new Hashtable<>();
        dn = StringUtils.strip(dn, "\"'");
        environment.put(Context.SECURITY_CREDENTIALS, pwd);
        environment.put(Context.SECURITY_PRINCIPAL, dn);
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, url);
        environment.put("com.sun.jndi.ldap.connect.timeout", getLdapConnTimeout());
        environment.put("com.sun.jndi.ldap.read.timeout", getLdapConnReadTimeout());
        // 1.2. Init ssl env if necessary.
        if (!isLdapSslConnAllowInsecure()) {
            String trustStorePath = getLdapSslConnTrustStorePath();
            String trustStorePwd = getLdapSslConnTrustStorePwd();
            SSLContext sslContext = SslUtils.createSSLContext(
                    Optional.empty(), /* For now, we don't support server to verify us(client). */
                    Optional.empty(),
                    trustStorePath.isEmpty() ? Optional.empty() : Optional.of(new File(trustStorePath)),
                    trustStorePwd.isEmpty() ? Optional.empty() : Optional.of(trustStorePwd));
            LdapSslSocketFactory.setSslContextForCurrentThread(sslContext);
            // Refer to https://docs.oracle.com/javase/jndi/tutorial/ldap/security/ssl.html.
            environment.put("java.naming.ldap.factory.socket", LdapSslSocketFactory.class.getName());
        }

        // 2. Create dir context and return.
        return new InitialDirContext(environment);
    }
}
