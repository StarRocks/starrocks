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
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.UserIdentity;
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
import javax.naming.SizeLimitExceededException;
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

    /**
     * How long, in seconds, the last successfully built cache may keep being served while refreshes
     * keep failing. Once the cache has been stale for longer than this, it is dropped and every user
     * resolves to an empty group set.
     * <p>
     * Dropping the cache immediately on the first failure turns a brief LDAP outage into a
     * cluster-wide login failure whenever `permitted_groups` is configured, because an empty group
     * set can never intersect the allowed list. Serving a slightly stale cache for a bounded period
     * is the safer trade-off. Set to 0 to drop the cache as soon as a refresh fails.
     */
    public static final String LDAP_CACHE_MAX_STALE_TIME = "ldap_cache_max_stale_time";

    public static final Set<String> KNOWN_PROPERTY_KEYS = ImmutableSet.of(
            GROUP_PROVIDER_PROPERTY_TYPE_KEY,
            LDAP_LDAP_CONN_URL,
            LDAP_PROP_ROOT_DN_KEY,
            LDAP_PROP_ROOT_PWD_KEY,
            LDAP_PROP_BASE_DN_KEY,
            LDAP_SSL_CONN_ALLOW_INSECURE,
            LDAP_SSL_CONN_TRUST_STORE_PATH,
            LDAP_SSL_CONN_TRUST_STORE_PWD,
            LDAP_PROP_CONN_TIMEOUT_MS_KEY,
            LDAP_PROP_CONN_READ_TIMEOUT_MS_KEY,
            LDAP_GROUP_FILTER,
            LDAP_GROUP_DN,
            LDAP_GROUP_IDENTIFIER_ATTR,
            LDAP_GROUP_MEMBER_ATTR,
            LDAP_USER_SEARCH_ATTR,
            LDAP_CACHE_REFRESH_INTERVAL,
            LDAP_CACHE_MAX_STALE_TIME);

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
     * Cache user-to-group mapping. Written by the refresh task, read by every login, hence volatile.
     */
    private volatile Map<String, Set<String>> userToGroupCache = new ConcurrentHashMap<>();

    /**
     * Wall-clock time of the last refresh that actually reached the directory, or 0 if none ever did.
     * Used together with {@link #LDAP_CACHE_MAX_STALE_TIME} to decide whether a failed refresh may
     * keep serving the previous cache.
     */
    private volatile long lastSuccessfulRefreshTimeMs = 0;

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
        // A provider activated by ALTER has just loaded the whole directory synchronously in
        // prepareForActivation(); starting the schedule at delay 0 would walk it again milliseconds later
        // for nothing. Every other entry point (CREATE, replay, restart) arrives with a cold cache and
        // does need that first refresh now.
        long initialDelaySeconds = cacheWarmedByActivation ? getLdapCacheRefreshInterval() : 0;
        scheduleTask = SCHEDULER.scheduleAtFixedRate(this::refreshGroups, initialDelaySeconds,
                getLdapCacheRefreshInterval(), TimeUnit.SECONDS);
    }

    /**
     * Forwards lookups to the outgoing instance until this one has loaded its own cache. Only the replay
     * path calls it: the leader's ALTER has already run {@link #prepareForActivation()} by the time it
     * publishes. If the outgoing instance is itself still bridging - two ALTERs replayed within one
     * refresh interval - point at what it is serving from, so the bridge never targets an instance whose
     * cache is still empty and never grows into a chain.
     */
    @Override
    public void serveFromUntilWarm(GroupProvider previous) {
        if (previous instanceof LDAPGroupProvider) {
            LDAPGroupProvider outgoing = (LDAPGroupProvider) previous;
            LDAPGroupProvider outgoingBridge = outgoing.servingUntilWarm;
            this.servingUntilWarm = outgoingBridge != null ? outgoingBridge : outgoing;
        }
    }

    @Override
    public void destroy() {
        // scheduleTask may be null if this provider was constructed but never init()'ed
        // (e.g. a freshly built provider whose ALTER failed before activation).
        if (scheduleTask != null) {
            scheduleTask.cancel(true);
        }
    }

    /**
     * Synchronously connect to the LDAP server and load the group cache once, validating that the
     * current configuration is usable before this provider is swapped into service by ALTER.
     * Throws {@link DdlException} if the server cannot be reached or the bind credentials are wrong,
     * so the ALTER fails fast and the old provider keeps serving. Does NOT start the periodic
     * refresh schedule; that is {@link #init()}'s responsibility, run at swap time.
     */
    @Override
    public void prepareForActivation() throws DdlException {
        Map<String, Set<String>> groups = new ConcurrentHashMap<>();
        try {
            fetchGroupsInto(groups);
        } catch (Exception e) {
            throw new DdlException("failed to apply the new configuration to group provider '" + name
                    + "': " + e.getMessage(), e);
        }
        this.userToGroupCache = groups;
        this.cacheWarmedByActivation = true;
        // Without this the cache we just built would count as never refreshed, so the first failed refresh
        // would compare against 0, find it stale beyond any ldap_cache_max_stale_time and drop it.
        this.lastSuccessfulRefreshTimeMs = System.currentTimeMillis();
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
        LDAPGroupProvider bridge = servingUntilWarm;
        if (bridge != null) {
            // Forward the query, not the cache: the outgoing instance resolves it under its own
            // configuration, so its key encoding is the one its cache was built with.
            return bridge.getGroup(userIdentity, distinguishedName);
        }
        String ldapUserSearchAttr = getLdapUserSearchAttr();
        String lookupKey;
        if (ldapUserSearchAttr != null) {
            // Normalize username for case-insensitive matching (LDAP is case-insensitive by default)
            lookupKey = LDAPAuthProvider.normalizeUsername(userIdentity.getUser());
        } else {
            // When using distinguished name, normalize it for case-insensitive matching
            // Without a search attribute the cache is keyed by the member DN, so canonicalize it: two
            // spellings of one DN differing only in separator whitespace or case must not land in two
            // different cache entries.
            lookupKey = LDAPAuthProvider.canonicalDn(distinguishedName);
        }
        return userToGroupCache.getOrDefault(lookupKey, Set.of());
    }

    public void refreshGroups() {
        LOG.info("refresh ldap group cache for group provider: {}", name);
        try {
            doRefreshGroups();
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
     * @return true if the whole directory was traversed; false if the answer was truncated - a referral the
     *         server will not chase, or a server-side entry cap - or if neither group property is set.
     */
    @VisibleForTesting
    boolean fetchGroupsInto(Map<String, Set<String>> groups)
            throws NamingException, IOException, GeneralSecurityException {
        // javax.naming.Context is not AutoCloseable, so this cannot be try-with-resources. The
        // context holds a live socket that JNDI does not reclaim on GC, so every path - including
        // the exceptional ones - has to reach the close, or one connection leaks per refresh and
        // per ALTER until the directory's per-client connection table fills up.
        DirContext ctx = createDirContextOnConnection(getLdapBindRootDn(), getLdapBindRootPwd());
        try {
            UserNameExtractInterface userNameExtractInterface = getUserNameExtractInterface();

            if (getLdapGroupFilter() != null) {
                SearchControls searchControls = new SearchControls();
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                NamingEnumeration<SearchResult> results =
                        ctx.search(getLdapBaseDn(), getLdapGroupFilter(), searchControls);
                try {
                    while (results.hasMore()) {
                        SearchResult result = results.next();
                        Attributes attributes = result.getAttributes();
                        matchUserAndUpdateGroups(groups, attributes, userNameExtractInterface);
                    }
                    return true;
                } catch (PartialResultException | SizeLimitExceededException e) {
                    // Both mean "the directory answered with less than everything": a referral it
                    // will not chase, or a server-side entry cap (Active Directory's MaxPageSize,
                    // default 1000) hit by a subtree search. The JDK defers the limit exception to
                    // the end of the enumeration, so what was already returned is usable - treat it
                    // as the best-effort partial result this method's contract promises, rather than
                    // failing the whole fetch and, through prepareForActivation(), the whole ALTER.
                    // Deliberately NOT the shared supertype LimitExceededException: its other
                    // subclass, TimeLimitExceededException, is a transient failure (server load, AD's
                    // MaxQueryDuration), so the operator can retry and get a complete answer. An
                    // entry cap is fixed configuration - every fetch hits it, so failing on it would
                    // disable ALTER on such a directory for good.
                    LOG.warn("LDAP group search returned a partial result for provider: {}", name, e);
                    return false;
                } finally {
                    closeQuietly(results);
                }
            } else if (getLdapGroupDn() != null) {
                for (String ldapGroupDN : getLdapGroupDn()) {
                    Attributes attributes = ctx.getAttributes(ldapGroupDN,
                            new String[] {getLdapGroupIdentifierAttr(), getLDAPGroupMemberAttr()});
                    matchUserAndUpdateGroups(groups, attributes, userNameExtractInterface);
                }
                return true;
            } else {
                LOG.warn("Neither ldap_group_filter nor ldap_group_dn exists");
                return false;
            }
        } finally {
            closeQuietly(ctx);
        }
    }

    private static void closeQuietly(Context ctx) {
        if (ctx == null) {
            return;
        }
        try {
            ctx.close();
        } catch (NamingException e) {
            LOG.warn("failed to close the LDAP context", e);
        }
    }

    private static void closeQuietly(NamingEnumeration<?> results) {
        if (results == null) {
            return;
        }
        try {
            results.close();
        } catch (NamingException e) {
            LOG.warn("failed to close the LDAP search enumeration", e);
        }
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
            // Same function the lookup side uses in getGroup(): a user name when a search attribute is
            // configured, the whole member DN otherwise.
            String normalizedUserName = getLdapUserSearchAttr() != null
                    ? LDAPAuthProvider.normalizeUsername(extractUserName)
                    : LDAPAuthProvider.canonicalDn(extractUserName);

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
    public Set<String> getKnownPropertyKeys() {
        return KNOWN_PROPERTY_KEYS;
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
        // Unvalidated, the refresh interval reaches scheduleAtFixedRate() as the period: 0 or a negative
        // value throws a message-less IllegalArgumentException out of init(), which is not a DdlException
        // and reaches the client as "Unknown error", and a non-numeric value throws a NumberFormatException
        // that names no property. Both after prepareForActivation() has already paid a full directory walk.
        validateIntegerProp(properties, LDAP_CACHE_REFRESH_INTERVAL, 1, Integer.MAX_VALUE);
        validateIntegerProp(properties, LDAP_CACHE_MAX_STALE_TIME, 0, Integer.MAX_VALUE);

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

    public long getLdapCacheMaxStaleTime() {
        return Long.parseLong(properties.getOrDefault(LDAP_CACHE_MAX_STALE_TIME, "3600"));
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

    @VisibleForTesting
    public void setLastSuccessfulRefreshTimeMs(long lastSuccessfulRefreshTimeMs) {
        this.lastSuccessfulRefreshTimeMs = lastSuccessfulRefreshTimeMs;
    }
}
