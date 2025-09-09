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
import com.starrocks.catalog.UserIdentityWithDnName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.net.ssl.SSLContext;

public class LDAPGroupProvider extends GroupProvider {
    private static final Logger LOG = LogManager.getLogger(LDAPGroupProvider.class);

    public static final String TYPE = "ldap";
    public static final String LDAP_LDAP_CONN_URL = "ldap_conn_url";
    public static final String LDAP_PROP_ROOT_DN_KEY = "ldap_bind_root_dn";
    public static final String LDAP_PROP_ROOT_PWD_KEY = "ldap_bind_root_pwd";
    public static final String LDAP_PROP_BASE_DN_KEY = "ldap_bind_base_dn";
    public static final String LDAP_SSL_CONN_ALLOW_INSECURE = "ldap_ssl_conn_allow_insecure";
    public static final String LDAP_SSL_CONN_TRUST_STORE_PATH = "ldap_ssl_conn_trust_store_path";
    public static final String LDAP_SSL_CONN_TRUST_STORE_PWD = "ldap_ssl_conn_trust_store_pwd";
    public static final String LDAP_PROP_CONN_TIMEOUT_MS_KEY = "ldap_conn_timeout";
    public static final String LDAP_PROP_CONN_READ_TIMEOUT_MS_KEY = "ldap_conn_read_timeout";

    /**
     * ldap_group_filter: sent directly to ldap server as filter
     * ldap_group_dn: specify the group dn to be searched
     * The two parameters ldap_group_filter and ldap_group_dn cannot be used at the same time.
     */
    public static final String LDAP_GROUP_FILTER = "ldap_group_filter";
    public static final String LDAP_GROUP_DN = "ldap_group_dn";

    /**
     * Specify which attr is used as the identifier of the tag group name
     */
    public static final String LDAP_GROUP_IDENTIFIER_ATTR = "ldap_group_identifier_attr";

    /**
     * Specify the type of member in the group, usually member or memberUid
     */
    public static final String LDAP_GROUP_MEMBER_ATTR = "ldap_group_member_attr";

    /**
     * Specify how to extract the user identifier from the member value.
     * You can explicitly specify the attribute (such as cn, uid) or use regular expressions.
     */
    public static final String LDAP_USER_SEARCH_ATTR = "ldap_user_search_attr";

    /**
     * Control the refresh frequency of ldap group
     */
    public static final String LDAP_CACHE_REFRESH_INTERVAL = "ldap_cache_refresh_interval";

    public static final Set<String> REQUIRED_PROPERTIES = new HashSet<>(Arrays.asList(
            LDAP_LDAP_CONN_URL,
            LDAP_PROP_ROOT_DN_KEY,
            LDAP_PROP_ROOT_PWD_KEY,
            LDAP_PROP_BASE_DN_KEY));

    /**
     * Used to refresh the ldap group cache. All ldap group providers share the same thread pool.
     */
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(Config.group_provider_refresh_thread_num);

    /**
     * Cache user-to-group mapping
     */
    private Map<String, Set<String>> userToGroupCache = new ConcurrentHashMap<>();

    /**
     * The current ldap group provider is registered to the scheduling task in the thread pool.
     * which is mainly used to cancel the periodic scheduling when the group provider is destroyed.
     */
    private ScheduledFuture<?> scheduleTask;

    public LDAPGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public void init() throws DdlException {
        scheduleTask = SCHEDULER.scheduleAtFixedRate(this::refreshGroups, 0, getLdapCacheRefreshInterval(), TimeUnit.SECONDS);
    }

    @Override
    public void destory() {
        scheduleTask.cancel(true);
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity) {
        Set<String> userGroups = userToGroupCache.get(userIdentity.getUser());

        if (userGroups == null && userIdentity instanceof UserIdentityWithDnName identityWithDnName) {
            if (identityWithDnName.getDnName() != null) {
                userGroups = userToGroupCache.get(identityWithDnName.getDnName());
            }
        }

        return Objects.requireNonNullElse(userGroups, Set.of());
    }

    public void refreshGroups() {
        Map<String, Set<String>> groups = new ConcurrentHashMap<>();
        try {
            DirContext ctx = createDirContextOnConnection(getLdapBindRootDn(), getLdapBindRootPwd());
            UserNameExtractInterface userNameExtractInterface = getUserNameExtractInterface();

            if (getLdapGroupFilter() != null) {
                SearchControls searchControls = new SearchControls();
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                NamingEnumeration<SearchResult> results = ctx.search(getLdapBaseDn(), getLdapGroupFilter(), searchControls);
                try {
                    while (results.hasMore()) {
                        SearchResult result = results.next();
                        Attributes attributes = result.getAttributes();
                        matchUserAndUpdateGroups(groups, attributes, userNameExtractInterface);
                    }
                } catch (PartialResultException e) {
                    LOG.warn("LDAP group search partial result exception", e);
                }
            } else if (getLdapGroupDn() != null) {
                for (String ldapGroupDN : getLdapGroupDn()) {
                    Attributes attributes =
                            ctx.getAttributes(ldapGroupDN, new String[] {getLdapGroupIdentifierAttr(), getLDAPGroupMemberAttr()});
                    matchUserAndUpdateGroups(groups, attributes, userNameExtractInterface);
                }
            } else {
                LOG.warn("Neither ldap_group_filter nor ldap_group_dn exists");
            }
        } catch (Exception e) {
            //Do not affect the normal login process at this time. If an error occurs, an empty group will be returned.
            LOG.error("LDAP group search failed", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("LDAP group refresh completed, userToGroupCache: {}", groups);
        }

        this.userToGroupCache = groups;
    }

    private void matchUserAndUpdateGroups(Map<String, Set<String>> groups,
                                          Attributes attributes,
                                          UserNameExtractInterface userNameExtractInterface)
            throws NamingException {
        Attribute ldapGroupIdentifierAttr = attributes.get(getLdapGroupIdentifierAttr());
        if (ldapGroupIdentifierAttr == null) {
            LOG.warn("LDAP group identifier attribute '{}' not found in attributes: {}",
                    getLdapGroupIdentifierAttr(), attributes);
            return;
        }
        String groupName = (String) ldapGroupIdentifierAttr.get();

        Attribute memberAttribute = attributes.get(getLDAPGroupMemberAttr());
        if (memberAttribute == null) {
            LOG.warn("LDAP group member attribute '{}' not found in attributes: {}", getLDAPGroupMemberAttr(), attributes);
            return;
        }

        NamingEnumeration<?> e = memberAttribute.getAll();
        while (e.hasMore()) {
            String memberDN = (String) e.next();
            String extractUserName = userNameExtractInterface.extract(memberDN);

            if (extractUserName == null) {
                LOG.debug("Failed to extract user name from member DN: '{}'", memberDN);
                continue;
            }

            groups.putIfAbsent(extractUserName, new HashSet<>());
            groups.get(extractUserName).add(groupName);

            LOG.debug("Successfully extracted user '{}' from member '{}', added to group '{}'",
                    extractUserName, memberDN, groupName);
        }
    }

    @FunctionalInterface
    private interface UserNameExtractInterface {
        String extract(String dn);
    }

    private UserNameExtractInterface getUserNameExtractInterface() {
        UserNameExtractInterface userNameExtractInterface;
        String ldapUserSearchAttr = getLdapUserSearchAttr();

        if (ldapUserSearchAttr != null) {
            Pattern pattern = Pattern.compile(ldapUserSearchAttr);
            if (pattern.matcher("").groupCount() == 0) {
                userNameExtractInterface = memberDn -> {
                    String[] splits = memberDn.split(",\\s*");
                    for (String split : splits) {
                        if (split.startsWith(ldapUserSearchAttr + "=")) {
                            String matchedName;
                            try {
                                matchedName = split.substring(split.indexOf("=") + 1);
                            } catch (IndexOutOfBoundsException e) {
                                LOG.warn("invalid member name format: '{}', msg: {}", memberDn, e.getMessage());
                                return null;
                            }
                            LOG.info("found matched member name '{}' from member '{}'", matchedName, memberDn);
                            return matchedName;
                        }
                    }

                    LOG.debug("skip member '{}' because it does not match the search attr '{}'", memberDn, ldapUserSearchAttr);
                    return null;
                };
            } else {
                userNameExtractInterface = memberDN -> {
                    Matcher matcher = pattern.matcher(memberDN);
                    if (matcher.find()) {
                        return matcher.group(1);
                    } else {
                        LOG.debug("skip member '{}' because it does not match the search attr '{}'", memberDN,
                                ldapUserSearchAttr);
                        return null;
                    }
                };
            }
        } else {
            userNameExtractInterface = memberDn -> memberDn;
        }

        return userNameExtractInterface;
    }

    @Override
    public void checkProperty() throws SemanticException {
        REQUIRED_PROPERTIES.forEach(s -> {
            if (!properties.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });

        validateIntegerProp(properties, LDAP_PROP_CONN_TIMEOUT_MS_KEY,
                10, Integer.MAX_VALUE);
        validateIntegerProp(properties, LDAP_PROP_CONN_READ_TIMEOUT_MS_KEY,
                10, Integer.MAX_VALUE);

        if ((properties.get(LDAP_GROUP_DN) == null && properties.get(LDAP_GROUP_FILTER) == null) ||
                (properties.get(LDAP_GROUP_DN) != null && properties.get(LDAP_GROUP_FILTER) != null)) {
            throw new SemanticException("ldap_group_dn and ldap_group_filter can use either one at the same time");
        }
    }

    public DirContext createDirContextOnConnection(String dn, String pwd) throws NamingException, IOException,
            GeneralSecurityException {
        if (Strings.isNullOrEmpty(pwd)) {
            LOG.warn("empty password is not allowed for simple authentication");
            throw new IOException("empty password is not allowed for simple authentication");
        }

        String url = getLdapConnUrl();
        Hashtable<String, String> environment = new Hashtable<>();
        dn = StringUtils.strip(dn, "\"'");
        environment.put(Context.SECURITY_CREDENTIALS, pwd);
        environment.put(Context.SECURITY_PRINCIPAL, dn);
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, url);
        environment.put("com.sun.jndi.ldap.connect.timeout", getLdapConnTimeout());
        environment.put("com.sun.jndi.ldap.read.timeout", getLdapConnReadTimeout());

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

        return new InitialDirContext(environment);
    }

    public String getLdapConnUrl() {
        return properties.getOrDefault(LDAP_LDAP_CONN_URL, "");
    }

    public String getLdapBindRootDn() {
        return properties.get(LDAP_PROP_ROOT_DN_KEY);
    }

    public String getLdapBindRootPwd() {
        return properties.get(LDAP_PROP_ROOT_PWD_KEY);
    }

    public String getLdapBaseDn() {
        return properties.get(LDAP_PROP_BASE_DN_KEY);
    }

    public String getLdapConnTimeout() {
        return properties.getOrDefault(LDAP_PROP_CONN_TIMEOUT_MS_KEY, "30000");
    }

    public String getLdapConnReadTimeout() {
        return properties.getOrDefault(LDAP_PROP_CONN_READ_TIMEOUT_MS_KEY, "30000");
    }

    public boolean isLdapSslConnAllowInsecure() {
        return Boolean.parseBoolean(properties.getOrDefault(LDAP_SSL_CONN_ALLOW_INSECURE, "true"));
    }

    public String getLdapSslConnTrustStorePath() {
        return properties.getOrDefault(LDAP_SSL_CONN_TRUST_STORE_PATH, "");
    }

    public String getLdapSslConnTrustStorePwd() {
        return properties.getOrDefault(LDAP_SSL_CONN_TRUST_STORE_PWD, "");
    }

    public String getLdapGroupFilter() {
        return properties.get(LDAP_GROUP_FILTER);
    }

    public List<String> getLdapGroupDn() {
        if (properties.get(LDAP_GROUP_DN) == null) {
            return null;
        } else {
            return List.of(properties.get(LDAP_GROUP_DN).split(";\\s*"));
        }
    }

    public String getLdapGroupIdentifierAttr() {
        return properties.getOrDefault(LDAP_GROUP_IDENTIFIER_ATTR, "cn");
    }

    public String getLDAPGroupMemberAttr() {
        return properties.getOrDefault(LDAP_GROUP_MEMBER_ATTR, "member");
    }

    public String getLdapUserSearchAttr() {
        return properties.get(LDAP_USER_SEARCH_ATTR);
    }

    public Long getLdapCacheRefreshInterval() {
        return Long.parseLong(properties.getOrDefault(LDAP_CACHE_REFRESH_INTERVAL, "300"));
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
}
