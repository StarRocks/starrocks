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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
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
<<<<<<< HEAD
    private Map<String, Set<String>> userToGroupCache = new ConcurrentHashMap<>();
=======
    private volatile Map<String, Set<String>> userToGroupCache = new ConcurrentHashMap<>();

    /**
     * Wall-clock time of the last refresh that actually reached the directory, or 0 if none ever did.
     * Used together with {@link #LDAP_CACHE_MAX_STALE_TIME} to decide whether a failed refresh may
     * keep serving the previous cache.
     */
    private volatile long lastSuccessfulRefreshTimeMs = 0;

    private static final long NOT_LOADED_WARN_INTERVAL_MS = 60_000L;

    /**
     * When {@link #getGroup} last logged that this provider has not loaded yet. Only throttles that log.
     */
    private volatile long lastNotLoadedWarnTimeMs = 0;

    /**
     * True once {@link #prepareForActivation()} has filled {@link #userToGroupCache} synchronously.
     * Read by {@link #init()} to decide whether the periodic refresh has to run immediately.
     */
    private boolean cacheWarmedByActivation = false;

    /**
     * The instance this one replaced on the replay path, answering lookups on its behalf until this
     * instance's first refresh has completed (see {@link #serveFromUntilWarm}). Null once that refresh
     * has ended, whether it succeeded or not. Written on the replay thread and on the refresh thread,
     * read on every login, hence volatile.
     */
    private volatile LDAPGroupProvider servingUntilWarm;
>>>>>>> f9cd04d ([BugFix] Keep LDAP group provider usable after FE restart and AD referrals (#80361))

    /**
     * The current ldap group provider is registered to the scheduling task in the thread pool.
     * which is mainly used to cancel the periodic scheduling when the group provider is destroyed.
     */
    private ScheduledFuture<?> scheduleTask;

    public LDAPGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    /**
     * For Gson. A provider loaded from the image must still run the field initializers above: without this
     * constructor {@link #userToGroupCache} stays null until the first successful refresh, and every login
     * in between fails with a NullPointerException.
     */
    private LDAPGroupProvider() {
    }

    @Override
    public void init() throws DdlException {
        scheduleTask = SCHEDULER.scheduleAtFixedRate(this::refreshGroups, 0, getLdapCacheRefreshInterval(), TimeUnit.SECONDS);
    }

    @Override
    public void destroy() {
        scheduleTask.cancel(true);
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
        String ldapUserSearchAttr = getLdapUserSearchAttr();
        String lookupKey;
        if (ldapUserSearchAttr != null) {
            // Normalize username for case-insensitive matching (LDAP is case-insensitive by default)
            lookupKey = LDAPAuthProvider.normalizeUsername(userIdentity.getUser());
        } else {
            // When using distinguished name, normalize it for case-insensitive matching
            lookupKey = LDAPAuthProvider.normalizeUsername(distinguishedName);
        }
        // 0 means no refresh has succeeded since this instance was created or restored from the image (the
        // field is not persisted), so the cache is still empty because it was never loaded, not because the
        // user belongs to no group.
        if (lastSuccessfulRefreshTimeMs == 0) {
            warnNotLoadedYet(userIdentity);
        }
        return userToGroupCache.getOrDefault(lookupKey, Set.of());
    }

    /**
     * Explains an empty group set during the window between FE start and the first successful refresh, so
     * a login refused by `permitted_groups` in that window is not mistaken for a membership problem.
     * Rate limited per provider: every login in the window hits this path.
     */
    private void warnNotLoadedYet(UserIdentity userIdentity) {
        long now = System.currentTimeMillis();
        if (now - lastNotLoadedWarnTimeMs < NOT_LOADED_WARN_INTERVAL_MS) {
            return;
        }
        lastNotLoadedWarnTimeMs = now;
        LOG.warn("group provider '{}' has not completed a successful load since this FE started; user '{}' " +
                "resolves to no groups through it until a refresh succeeds", name, userIdentity.getUser());
    }

    public void refreshGroups() {
        LOG.info("refresh ldap group cache for group provider: {}", name);
<<<<<<< HEAD
        Map<String, Set<String>> groups = new ConcurrentHashMap<>();
=======
        try {
            doRefreshGroups();
        } catch (Throwable t) {
            // This runs as a scheduleAtFixedRate task, and the executor cancels every later run of a task
            // that throws. Letting anything escape would leave this provider never refreshed again until
            // the FE restarts.
            LOG.error("unexpected error while refreshing group provider '{}'; retrying at the next interval",
                    name, t);
        } finally {
            if (servingUntilWarm != null) {
                // The first refresh has ended, so this instance now stands on its own. If the refresh
                // failed the cache is empty and stays empty until one succeeds: the journal has already
                // committed this configuration, and answering from the retired one would grant groups
                // the cluster no longer defines - denying access is the smaller of the two wrong answers.
                if (userToGroupCache.isEmpty()) {
                    LOG.error("group provider '{}' could not load its cache on its first refresh; this node " +
                            "resolves no groups through it until a refresh succeeds", name);
                }
                servingUntilWarm = null;
            }
        }
    }

    private void doRefreshGroups() {
        Map<String, Set<String>> groups = new ConcurrentHashMap<>();
        boolean refreshed = false;
        try {
            // A truncated answer is NOT a successful refresh: publishing the partial map and stamping the
            // success timestamp would replace a complete cache with a smaller one and close the
            // ldap_cache_max_stale_time window, so users whose groups sat in the untraversed part would
            // resolve to an empty set immediately. Leaving `refreshed` false keeps the last complete
            // cache until it goes stale.
            refreshed = fetchGroupsInto(groups);
        } catch (Exception e) {
            LOG.error("LDAP group search failed for group provider: {}", name, e);
        }

        if (refreshed) {
            this.userToGroupCache = groups;
            this.lastSuccessfulRefreshTimeMs = System.currentTimeMillis();
            if (LOG.isDebugEnabled()) {
                LOG.debug("LDAP group refresh completed, userToGroupCache: {}", groups);
            }
            return;
        }

        // The refresh failed. Keep serving the previous cache until it has been stale for longer than
        // `ldap_cache_max_stale_time`, so that a brief directory outage does not lock every user out
        // (an empty group set can never intersect a configured `permitted_groups` list).
        long maxStaleMs = getLdapCacheMaxStaleTime() * 1000L;
        long staleForMs = System.currentTimeMillis() - lastSuccessfulRefreshTimeMs;
        if (staleForMs > maxStaleMs) {
            if (!userToGroupCache.isEmpty()) {
                LOG.warn("LDAP group cache of group provider '{}' has been stale for {}ms which exceeds {}={}s, " +
                                "dropping {} cached entries; users will resolve to an empty group set",
                        name, staleForMs, LDAP_CACHE_MAX_STALE_TIME, getLdapCacheMaxStaleTime(), userToGroupCache.size());
            }
            this.userToGroupCache = new ConcurrentHashMap<>();
        } else {
            LOG.warn("LDAP group refresh failed for group provider '{}', keeping the last successful cache " +
                            "({} entries, stale for {}ms, {}={}s)",
                    name, userToGroupCache.size(), staleForMs, LDAP_CACHE_MAX_STALE_TIME, getLdapCacheMaxStaleTime());
        }
    }

    /**
     * Connect to the LDAP server and populate {@code groups} with the current user-to-group mapping.
     * Shared by {@link #refreshGroups()} and {@link #prepareForActivation()}, which disagree on what to do
     * with an incomplete answer: the refresh keeps its last complete cache (see
     * {@link #LDAP_CACHE_MAX_STALE_TIME}), while activation accepts what it got, because a directory that
     * always truncates would otherwise make ALTER impossible on it.
     *
     * <p>Only the two exceptions that come with usable data (referrals, entry cap) are handled here. Any other
     * failure - directory unreachable, bind refused, TLS error - is thrown on purpose, because the callers
     * react to it differently: the refresh logs it and keeps its last cache, while activation turns it into a
     * DdlException so the ALTER fails and the old provider keeps serving.
     *
     * @return true if the whole directory was traversed, counting an answer that only skipped referrals as
     *         whole; false if the answer was truncated by a server-side entry cap, or if neither group
     *         property is set.
     */
    @VisibleForTesting
    boolean fetchGroupsInto(Map<String, Set<String>> groups)
            throws NamingException, IOException, GeneralSecurityException {
        // javax.naming.Context is not AutoCloseable, so this cannot be try-with-resources. The
        // context holds a live socket that JNDI does not reclaim on GC, so every path - including
        // the exceptional ones - has to reach the close, or one connection leaks per refresh and
        // per ALTER until the directory's per-client connection table fills up.
        DirContext ctx = createDirContextOnConnection(getLdapBindRootDn(), getLdapBindRootPwd());
>>>>>>> f9cd04d ([BugFix] Keep LDAP group provider usable after FE restart and AD referrals (#80361))
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
<<<<<<< HEAD
                } catch (PartialResultException e) {
                    LOG.warn("LDAP group search partial result exception", e);
=======
                    return true;
                } catch (PartialResultException e) {
                    // Referrals, not truncation. Active Directory answers a subtree search from the domain
                    // root with continuation references to its other partitions (DomainDnsZones,
                    // ForestDnsZones, Configuration), on every search. JNDI does not follow them by
                    // default and reports them with this exception at the end of the enumeration, after
                    // every entry this server holds has been returned. Those partitions hold no groups,
                    // so the answer is complete for a group search. Groups kept in another domain of the
                    // forest are not found this way.
                    //
                    // Do NOT turn this back into a failed refresh (return false). Because such a directory
                    // returns referrals on every search, no refresh would ever succeed: the provider would
                    // never fill its cache, every user would resolve to no groups, and with
                    // `permitted_groups` configured every LDAP login would be refused. That is exactly what
                    // happened once this was treated as a failure, and it is what
                    // LDAPGroupProviderReferralTest guards.
                    LOG.warn("LDAP group search for provider '{}' returned referrals, which are not followed; " +
                            "using the entries returned by this server as the complete result: {}",
                            name, e.getMessage());
                    return true;
                } catch (SizeLimitExceededException e) {
                    // A server-side entry cap (Active Directory's MaxPageSize, default 1000) hit by a
                    // subtree search: the answer really is truncated. The JDK defers the limit exception
                    // to the end of the enumeration, so what was already returned is usable - treat it as
                    // the best-effort partial result this method's contract promises, rather than failing
                    // the whole fetch and, through prepareForActivation(), the whole ALTER.
                    // Deliberately NOT the shared supertype LimitExceededException: its other subclass,
                    // TimeLimitExceededException, is a transient failure (server load, AD's
                    // MaxQueryDuration), so the operator can retry and get a complete answer. An entry
                    // cap is fixed configuration - every fetch hits it, so failing on it would disable
                    // ALTER on such a directory for good.
                    LOG.warn("LDAP group search returned a truncated result for provider: {}", name, e);
                    return false;
                } finally {
                    closeQuietly(results);
>>>>>>> f9cd04d ([BugFix] Keep LDAP group provider usable after FE restart and AD referrals (#80361))
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

            // Normalize extracted username for case-insensitive matching
            // LDAP is case-insensitive by default, so we normalize to ensure consistent mapping
            String normalizedUserName = LDAPAuthProvider.normalizeUsername(extractUserName);

            groups.putIfAbsent(normalizedUserName, new HashSet<>());
            groups.get(normalizedUserName).add(groupName);

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

    @VisibleForTesting
    public void setUserToGroupCache(Map<String, Set<String>> userToGroupCache) {
        this.userToGroupCache = userToGroupCache;
    }
}
