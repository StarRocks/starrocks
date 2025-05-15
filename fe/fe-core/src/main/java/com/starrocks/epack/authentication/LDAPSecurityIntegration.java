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
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.common.Pair;
import com.starrocks.epack.security.SslUtils;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.net.ssl.SSLContext;

/**
 * Security integration specified in `Config.authentication_chain`.
 * Authentication for this integration is provided by member `authenticationProvider`.
 *
 * LDAP Security Integration has been deprecated and will be removed in subsequent versions
 */
@Deprecated
public class LDAPSecurityIntegration extends SecurityIntegration {
    private static final Logger LOG = LogManager.getLogger(LDAPSecurityIntegration.class);

    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY = "ldap_bind_root_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY = "ldap_bind_root_pwd";
    public static final String LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY = "ldap_cache_refresh_interval";
    public static final String LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_HOST = "ldap_server_host";
    public static final String LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_PORT = "ldap_server_port";
    public static final String LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL = "ldap_conn_url";

    public static final String LDAP_SEC_INTEGRATION_PROP_SSL_CONN_ALLOW_INSECURE = "ldap_ssl_conn_allow_insecure";
    public static final String LDAP_SEC_INTEGRATION_PROP_SSL_CONN_TRUST_STORE_PATH = "ldap_ssl_conn_trust_store_path";
    public static final String LDAP_SEC_INTEGRATION_PROP_SSL_CONN_TRUST_STORE_PWD = "ldap_ssl_conn_trust_store_pwd";
    public static final String LDAP_SEC_INTEGRATION_PROP_CONN_TIMEOUT_MS_KEY = "ldap_conn_timeout";
    public static final String LDAP_SEC_INTEGRATION_PROP_CONN_READ_TIMEOUT_MS_KEY = "ldap_conn_read_timeout";

    //========================  The following are the search related parameters  ==========================
    public static final String LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY = "ldap_bind_base_dn";
    public static final String LDAP_SEC_INTEGRATION_PROP_USER_SEARCH_ATTR = "ldap_user_search_attr";
    /**
     * If the user's attribute as the member of a group is different from the user's DN, you must specify this parameter.
     */
    public static final String LDAP_SEC_INTEGRATION_GROUP_MATCH_ATTR_KEY = "ldap_user_group_match_attr";
    /**
     * When `ldap_group_match_use_member_uid` set to "false",
     * we will not retrieve the member of the group based on `memberUid` attribute.
     */
    public static final String LDAP_SEC_INTEGRATION_PROP_USE_MEMBER_UID_KEY = "ldap_group_match_use_member_uid";
    /**
     * The attribute id of group member. Default is "member"
     */
    public static final String LDAP_SEC_INTEGRATION_PROP_LDAP_GROUP_MEMBER_ATTR = "ldap_group_member_attr";

    public static final String LDAP_USER_SEARCH_DEFAULT_ATTR = "uid";

    private static final String LDAPS_SERVER_SSL_DEFAULT_PORT = "636";

    final Set<String> requiredProperties = new HashSet<>(Arrays.asList(
            SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
            LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY,
            LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY,
            LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY));

    /**
     * last refresh time of group membership for all role
     * mappings connected with this security integration.
     */
    private long lastRefreshTime = -1;

    private volatile boolean isNetWorkReachable = true;

    public LDAPSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
        return new LDAPAuthProviderForExternal(name);
    }

    public String getLdapServerHost() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_HOST, "127.0.0.1");
    }

    public String getLdapServerPort() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_PORT, "389");
    }

    public String getLdapConnUrl() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "");
    }

    public boolean isLdapSslConnAllowInsecure() {
        return Boolean.parseBoolean(
                propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_SSL_CONN_ALLOW_INSECURE, "true"));
    }

    public String getLdapSslConnTrustStorePath() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_SSL_CONN_TRUST_STORE_PATH, "");
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
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_USER_SEARCH_ATTR, LDAP_USER_SEARCH_DEFAULT_ATTR);
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

    public String getLdapGroupMemberAttr() {
        return propertyMap.getOrDefault(LDAP_SEC_INTEGRATION_PROP_LDAP_GROUP_MEMBER_ATTR, "member");
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

    public void setNetWorkReachable(boolean netWorkReachable) {
        isNetWorkReachable = netWorkReachable;
    }

    public Pair<String, Integer> getHostAndPort() {
        if (getLdapConnUrl().isEmpty()) {
            try {
                return Pair.create(getLdapServerHost(), Integer.parseInt(getLdapServerPort()));
            } catch (NumberFormatException e) {
                LOG.warn("parse {} to number failed", getLdapServerPort());
                return null;
            }
        } else {
            String url = getLdapConnUrl();
            String[] schemeHostPort = url.split("://");
            if (schemeHostPort.length != 2) {
                LOG.warn("invalid url format: {}", url);
                return null;
            }
            String[] hostPort = schemeHostPort[1].split(":");
            if (hostPort.length != 2) {
                LOG.warn("get host:port from url: {} failed", url);
                return null;
            }
            try {
                return Pair.create(hostPort[0], Integer.parseInt(hostPort[1]));
            } catch (NumberFormatException e) {
                LOG.warn("parse {} to number failed", hostPort[1]);
                return null;
            }
        }
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
        if (!isNetWorkReachable) {
            LOG.warn("network not reachable, url: {}, name: {}", getLdapConnUrl(), name);
            return null;
        }

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

    @Override
    public void checkProperty() throws SemanticException {
        requiredProperties.forEach(s -> {
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

        validateIntegerProp(propertyMap, LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY,
                1, 86400 /* max one day in sec */);
        validateIntegerProp(propertyMap, LDAP_SEC_INTEGRATION_PROP_CONN_TIMEOUT_MS_KEY,
                10, Integer.MAX_VALUE);
        validateIntegerProp(propertyMap, LDAP_SEC_INTEGRATION_PROP_CONN_READ_TIMEOUT_MS_KEY,
                10, Integer.MAX_VALUE);
        validateBooleanProp(propertyMap, LDAP_SEC_INTEGRATION_PROP_USE_MEMBER_UID_KEY);
        validateBooleanProp(propertyMap, LDAP_SEC_INTEGRATION_PROP_SSL_CONN_ALLOW_INSECURE);

        if (propertyMap.containsKey(LDAP_SEC_INTEGRATION_GROUP_MATCH_ATTR_KEY)) {
            String value = propertyMap.get(LDAP_SEC_INTEGRATION_GROUP_MATCH_ATTR_KEY);
            if (value.startsWith("regex:") && value.split(":").length < 2) {
                throw new SemanticException("invalid '" +
                        LDAP_SEC_INTEGRATION_GROUP_MATCH_ATTR_KEY +
                        "' property value: " + value);
            }
        }
    }

    private void validateIntegerProp(Map<String, String> propertyMap, String key, int min, int max)
            throws SemanticException {
        if (propertyMap.containsKey(key)) {
            String val = propertyMap.get(key);
            try {
                int intVal = Integer.parseInt(val);
                if (intVal < min || intVal > max) {
                    throw new NumberFormatException("current value of '" +
                            key + "' is invalid, value: " + intVal +
                            ", should be in range [" + min + ", " + max + "]");
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("invalid '" +
                        key + "' property value: " + val + ", error: " + e.getMessage(), e);
            }
        }
    }

    private void validateBooleanProp(Map<String, String> propertyMap, String key) throws SemanticException {
        if (propertyMap.containsKey(key)) {
            String val = propertyMap.get(key);
            if (!val.equalsIgnoreCase("true") && !val.equalsIgnoreCase("false")) {
                throw new SemanticException("invalid '" +
                        key + "' property value, expected 'true' or 'false'");
            }
        }

    }
}
