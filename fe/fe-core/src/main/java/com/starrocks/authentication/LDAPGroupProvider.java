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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;
import javax.net.ssl.SSLContext;

public class LDAPGroupProvider extends GroupProvider implements GsonPostProcessable {
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
     * Matches the range suffix Active Directory encodes into the attribute id itself, e.g.
     * {@code member;range=0-1499} or {@code member;range=1500-*}. Tolerates other attribute options
     * around it, such as {@code member;lang-en;range=0-1499}.
     */
    private static final Pattern RANGE_PATTERN = Pattern.compile("(?i)(?:^|;)range=(\\d+)-(\\d+|\\*)(?:;|$)");

    /**
     * Hard cap on follow-up range pages fetched per group, so a server that keeps advertising a
     * non-terminal range cannot spin us forever. At AD's typical MaxValRange of 1500 this covers
     * ~150k members, far above any realistic group.
     */
    @VisibleForTesting
    static int MAX_RANGE_PAGES = 100;

    /**
     * Hard cap on paged search rounds per refresh, so a server that keeps handing back a non-empty cookie
     * cannot spin us forever. At the default page size of 1000 this covers ten million group entries.
     */
    @VisibleForTesting
    static int MAX_SEARCH_PAGES = 10_000;

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
     * Cache user-to-group mapping. Volatile because the refreshing thread swaps the whole reference while
     * authentication threads read it; ConcurrentHashMap alone would not make that assignment visible.
     *
     * <p>NOTE: none of the runtime state below survives a metadata image round trip. GSON instantiates a
     * provider loaded from the image through UnsafeAllocator, which skips both the constructor and these
     * inline initializers - and {@code HiddenAnnotationExclusionStrategy} skips every field without
     * {@code @SerializedName}, so nothing in the JSON fills them back in either. They arrive as null / 0.
     * {@link #gsonPostProcess()} is what restores them; see the comment there before adding a field here.
     */
    private volatile Map<String, Set<String>> userToGroupCache = Map.of();

    /** Health of the cache above, surfaced through SHOW GROUP PROVIDERS and the refresh log. */
    private volatile long lastSuccessfulRefreshTimeMs = 0L;
    private volatile int lastPublishedGroupEntryCount = 0;
    private volatile String lastRefreshError = null;
    /**
     * A plain volatile int rather than an AtomicInteger on purpose: a primitive cannot be null, so it
     * needs nothing from {@link #gsonPostProcess()} to survive an image load. Only the single refresh
     * thread writes it, so read-modify-write here needs no atomicity.
     */
    private volatile int consecutiveFailureCount = 0;

    /**
     * The current ldap group provider is registered to the scheduling task in the thread pool.
     * which is mainly used to cancel the periodic scheduling when the group provider is destroyed.
     */
    private ScheduledFuture<?> scheduleTask;

    public LDAPGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    /**
     * Restore the runtime state that GSON could not.
     *
     * <p>A provider loaded from the metadata image is instantiated through GSON's UnsafeAllocator, which
     * runs neither the constructor nor the inline field initializers, and every field here is skipped by
     * {@code HiddenAnnotationExclusionStrategy} (no {@code @SerializedName}) so the JSON does not carry
     * them either. Without this hook {@code userToGroupCache} is null on such a provider, and the first
     * {@code SHOW GROUP PROVIDERS} or scheduled refresh dies with an NPE.
     *
     * <p>Prefer primitives for new runtime state - they cannot be null and need nothing here. If a new
     * field must be a reference type, initialize it below.
     */
    @Override
    public void gsonPostProcess() throws IOException {
        if (userToGroupCache == null) {
            userToGroupCache = Map.of();
        }
    }

    @Override
    public void init() throws DdlException {
        // scheduleWithFixedDelay, not scheduleAtFixedRate: a refresh that outlives the interval must not
        // queue up an immediate re-run and hammer the directory. Large directories can easily exceed the
        // default 300s once range retrieval and paged search are in play.
        scheduleTask =
                SCHEDULER.scheduleWithFixedDelay(this::refreshGroups, 0, getLdapCacheRefreshInterval(), TimeUnit.SECONDS);
    }

    @Override
    public void destroy() {
        // init() may have failed before the task was registered.
        if (scheduleTask != null) {
            scheduleTask.cancel(true);
        }
    }

    /**
     * Surfaces cache health in the COMMENT column of SHOW GROUP PROVIDERS. A stale cache is deliberately
     * not an error at query time, so this is the cheapest way for an operator to notice that refreshes
     * have been failing without adding a new schema or metric.
     */
    @Override
    public String getComment() {
        StringBuilder sb = new StringBuilder()
                .append("cachedUsers=").append(userToGroupCache.size())
                .append(", groupEntries=").append(lastPublishedGroupEntryCount)
                .append(", lastSuccessAgoSec=")
                .append(lastSuccessfulRefreshTimeMs == 0L ? "never" : cacheStaleForMs() / 1000)
                .append(", consecutiveFailures=").append(consecutiveFailureCount);
        String error = lastRefreshError;
        if (error != null) {
            sb.append(", lastError=").append(error);
        }
        return sb.toString();
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
        return userToGroupCache.getOrDefault(lookupKey, Set.of());
    }

    /**
     * Refresh the cache, but only replace it when the directory was read completely.
     * <p>An incomplete read is the failure mode this whole class has to avoid: publishing a partial
     * user-to-group map looks exactly like "these users are in no groups", which denies access fleet-wide.
     * A stale-but-complete cache is strictly better, so a failed refresh keeps the last known good one and
     * says so loudly.
     * <p>This method must never throw. {@link java.util.concurrent.ScheduledExecutorService} cancels all
     * future runs of a task that lets an exception escape, so an unchecked exception here would silently
     * freeze group refresh until the FE restarts.
     */
    public void refreshGroups() {
        // The whole body is guarded, not just collectGroups(): ScheduledExecutorService cancels every
        // future run of a task that lets anything escape, so a single throw here would silently stop
        // group refresh until the FE restarts. An earlier revision guarded only the collect call, and an
        // NPE in the publish/logging code below did exactly that.
        try {
            refreshGroupsUnsafe();
        } catch (Throwable t) {
            LOG.error("LDAP group refresh threw unexpectedly for group provider: {}, "
                    + "keeping the last known good cache", name, t);
        }
    }

    private void refreshGroupsUnsafe() {
        LOG.info("refresh ldap group cache for group provider: {}", name);
        long startMs = System.currentTimeMillis();

        RefreshResult result;
        try {
            result = collectGroups();
        } catch (Throwable t) {
            LOG.error("LDAP group collect threw unexpectedly for group provider: {}", name, t);
            result = RefreshResult.failed(String.valueOf(t));
        }

        // Blanking guard: a filter that suddenly matches no group at all is far more likely to be an
        // outage or a broken filter than a real directory in which every group vanished. Note this counts
        // group entries, not users, so a user legitimately removed from every group still propagates.
        boolean blanked = result.groupEntryCount == 0 && lastPublishedGroupEntryCount > 0;

        if (result.fatal || blanked) {
            int failures = ++consecutiveFailureCount;
            lastRefreshError = result.fatal ? result.failureReason : "group search matched 0 group entries";
            LOG.error("LDAP group refresh FAILED for group provider: {}, keeping the last known good cache. "
                            + "reason={}, consecutiveFailures={}, cacheStaleForMs={}, cachedUsers={}",
                    name, lastRefreshError, failures, cacheStaleForMs(), userToGroupCache.size());
            return;
        }

        this.userToGroupCache = freeze(result.userToGroups);
        lastPublishedGroupEntryCount = result.groupEntryCount;
        lastSuccessfulRefreshTimeMs = System.currentTimeMillis();
        consecutiveFailureCount = 0;
        lastRefreshError = null;

        if (result.groupEntryCount == 0) {
            LOG.warn("LDAP group refresh for group provider: {} matched no group entry. Check ldap_group_filter "
                    + "or ldap_group_dn - no user will resolve to any group.", name);
        }
        LOG.info("LDAP group refresh finished for group provider: {}, groupEntries={}, memberDnsRead={}, "
                        + "cachedUsers={}, softErrors={}, elapsedMs={}",
                name, result.groupEntryCount, result.memberDnsRead, this.userToGroupCache.size(),
                result.softErrorCount, System.currentTimeMillis() - startMs);
    }

    /**
     * Read every configured group out of the directory. Never throws: a failure is reported through
     * {@link RefreshResult#fatal} so the caller can decide whether the result is safe to publish.
     */
    @VisibleForTesting
    RefreshResult collectGroups() {
        Map<String, Set<String>> groups = new ConcurrentHashMap<>();
        // Phase A collects whatever the server returned inline and records the groups whose member list
        // was truncated by range retrieval. Phase B resolves those, and only those, afterwards.
        List<PendingMemberRange> pending = new ArrayList<>();
        Stats stats = new Stats();
        LdapContext ctx = null;
        try {
            ctx = createDirContextOnConnection(getLdapBindRootDn(), getLdapBindRootPwd());
            UserNameExtractInterface userNameExtractInterface = getUserNameExtractInterface();

            if (getLdapGroupFilter() != null) {
                // Ask for the identifier and member attributes explicitly. Relying on the server's default
                // attribute set is not deterministic on Active Directory, and being explicit is what makes AD
                // emit the member attribute in range-encoded form when the group exceeds MaxValRange.
                String[] returningAttrs = new String[] {getLdapGroupIdentifierAttr(), getLDAPGroupMemberAttr()};
                boolean searchComplete = searchGroupEntriesPaged(ctx, getLdapBaseDn(), getLdapGroupFilter(),
                        returningAttrs, Config.ldap_group_provider_search_page_size, result -> {
                            String groupDN = extractGroupDN(result, getLdapBaseDn());
                            try {
                                matchUserAndUpdateGroups(groups, groupDN, result.getAttributes(),
                                        userNameExtractInterface, pending, stats);
                            } catch (NamingException ne) {
                                // Isolate the failure to this group so the rest of the directory still refreshes,
                                // but only when the server gave a determinate answer about this entry.
                                if (!isDeterministicAbsence(ne)) {
                                    throw ne;
                                }
                                stats.softErrors++;
                                LOG.warn("Failed to process LDAP group with DN '{}', skipping", groupDN, ne);
                            }
                            stats.groupEntries++;
                        });
                if (!searchComplete) {
                    return RefreshResult.failed(
                            "group entry search stopped early with pages still pending, entries were lost");
                }
            } else if (getLdapGroupDn() != null) {
                for (String ldapGroupDN : getLdapGroupDn()) {
                    try {
                        Attributes attributes = ctx.getAttributes(ldapGroupDN,
                                new String[] {getLdapGroupIdentifierAttr(), getLDAPGroupMemberAttr()});
                        matchUserAndUpdateGroups(groups, ldapGroupDN, attributes, userNameExtractInterface, pending,
                                stats);
                        stats.groupEntries++;
                    } catch (NamingException ne) {
                        // Isolate the failure to this group so the other configured groups still refresh.
                        if (!isDeterministicAbsence(ne)) {
                            throw ne;
                        }
                        stats.softErrors++;
                        LOG.warn("Failed to fetch attributes for LDAP group '{}', skipping", ldapGroupDN, ne);
                    }
                }
            } else {
                LOG.warn("Neither ldap_group_filter nor ldap_group_dn exists");
            }

            if (!resolvePendingRanges(ctx, pending, groups, userNameExtractInterface, stats)) {
                return RefreshResult.failed("range retrieval did not reach the terminal page for every group");
            }
        } catch (Exception e) {
            LOG.error("LDAP group search failed for group provider: {}", name, e);
            return RefreshResult.failed(e.toString());
        } finally {
            closeDirContext(ctx);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("LDAP group collect completed, userToGroupCache: {}", groups);
        }
        return RefreshResult.collected(groups, stats);
    }

    /**
     * Whether the server answered authoritatively that the entry is not there, as opposed to failing to
     * answer. Only a determinate absence is safe to skip past; anything else means the read may have been
     * truncated, and a truncated read must not be published. The list is a whitelist on purpose - an
     * exception nobody has classified yet counts as a failure.
     */
    @VisibleForTesting
    static boolean isDeterministicAbsence(NamingException e) {
        return e instanceof NameNotFoundException || e instanceof InvalidNameException;
    }

    private long cacheStaleForMs() {
        return lastSuccessfulRefreshTimeMs == 0L ? -1L : System.currentTimeMillis() - lastSuccessfulRefreshTimeMs;
    }

    /**
     * Publish as an immutable snapshot. Combined with the volatile field this gives safe publication:
     * {@code ConcurrentHashMap} protects its own contents but not the reference assignment, and callers
     * of {@link #getGroup} only ever read.
     */
    private static Map<String, Set<String>> freeze(Map<String, Set<String>> groups) {
        Map<String, Set<String>> frozen = new HashMap<>();
        groups.forEach((user, memberOf) -> frozen.put(user, Set.copyOf(memberOf)));
        return Map.copyOf(frozen);
    }

    /** Mutable counters for one refresh. Confined to the refreshing thread. */
    @VisibleForTesting
    static final class Stats {
        int groupEntries;
        int softErrors;
        long memberDnsRead;
    }

    /** Outcome of one directory read, so the publish decision is a value rather than a side effect. */
    @VisibleForTesting
    static final class RefreshResult {
        final Map<String, Set<String>> userToGroups;
        final int groupEntryCount;
        final long memberDnsRead;
        final int softErrorCount;
        final boolean fatal;
        final String failureReason;

        private RefreshResult(Map<String, Set<String>> userToGroups, int groupEntryCount, long memberDnsRead,
                              int softErrorCount, boolean fatal, String failureReason) {
            this.userToGroups = userToGroups;
            this.groupEntryCount = groupEntryCount;
            this.memberDnsRead = memberDnsRead;
            this.softErrorCount = softErrorCount;
            this.fatal = fatal;
            this.failureReason = failureReason;
        }

        static RefreshResult collected(Map<String, Set<String>> userToGroups, Stats stats) {
            return new RefreshResult(userToGroups, stats.groupEntries, stats.memberDnsRead, stats.softErrors,
                    false, null);
        }

        static RefreshResult failed(String failureReason) {
            return new RefreshResult(Map.of(), 0, 0L, 0, true, failureReason);
        }
    }

    private void matchUserAndUpdateGroups(Map<String, Set<String>> groups,
                                          String groupDN,
                                          Attributes attributes,
                                          UserNameExtractInterface userNameExtractInterface,
                                          List<PendingMemberRange> pending,
                                          Stats stats)
            throws NamingException {
        Attribute ldapGroupIdentifierAttr = attributes.get(getLdapGroupIdentifierAttr());
        if (ldapGroupIdentifierAttr == null) {
            LOG.warn("LDAP group identifier attribute '{}' not found for group '{}', attributes: {}",
                    getLdapGroupIdentifierAttr(), groupDN, attributes);
            return;
        }
        String groupName = (String) ldapGroupIdentifierAttr.get();

        long nextStart = consumeMemberPage(attributes, getLDAPGroupMemberAttr(), memberDN -> {
            stats.memberDnsRead++;
            collectMember(groups, groupName, memberDN, userNameExtractInterface);
        });

        if (nextStart >= 0) {
            if (Strings.isNullOrEmpty(groupDN)) {
                // Without an absolute DN we cannot ask for the next page. Record it as a truncation
                // rather than pretending the group was read completely.
                LOG.warn("LDAP group '{}' is range-truncated at {} but its DN is unavailable, members will be incomplete",
                        groupName, nextStart);
                pending.add(new PendingMemberRange(null, groupName, nextStart));
            } else {
                pending.add(new PendingMemberRange(groupDN, groupName, nextStart));
            }
        }
    }

    /**
     * Resolve the groups whose member list Active Directory truncated via range retrieval.
     * <p>Runs strictly after the group entries have all been walked, so the context carries no
     * search-scoped request controls when these base-object reads go out.
     *
     * @return true when every pending group was read through to its terminal page.
     */
    private boolean resolvePendingRanges(DirContext ctx,
                                         List<PendingMemberRange> pending,
                                         Map<String, Set<String>> groups,
                                         UserNameExtractInterface userNameExtractInterface,
                                         Stats stats) {
        boolean complete = true;
        for (PendingMemberRange range : pending) {
            if (range.groupDN == null) {
                complete = false;
                continue;
            }
            LOG.debug("Fetching range retrieval pages for LDAP group '{}' starting at {}", range.groupDN, range.nextStart);
            boolean done = fetchRemainingMemberPages(ctx, range.groupDN, getLDAPGroupMemberAttr(), range.nextStart,
                    memberDN -> {
                        stats.memberDnsRead++;
                        collectMember(groups, range.groupName, memberDN, userNameExtractInterface);
                    });
            complete &= done;
        }
        return complete;
    }

    @FunctionalInterface
    interface SearchResultHandler {
        void handle(SearchResult result) throws NamingException;
    }

    /**
     * Walk the group entries matching {@code filter}, using the RFC 2696 simple paged results control so a
     * directory whose server-side size limit (MaxPageSize, 1000 on Active Directory) is smaller than the
     * result set still returns everything instead of failing with sizeLimitExceeded.
     * <p>The control is sent as {@link Control#NONCRITICAL}: a server that does not implement RFC 2696
     * ignores it and answers with the full result set and no response control, which collapses this into
     * the single unpaged search it replaces. {@code pageSize <= 0} skips the control entirely.
     * <p>The request controls are always cleared on the way out. They are scoped to the context, and a
     * cookie belonging to this search must not ride along on the base-object reads that range retrieval
     * issues afterwards.
     *
     * @return true when every page was walked to the end. false means the walk stopped early and the
     *         caller must not publish the result as a complete view of the directory.
     */
    @VisibleForTesting
    static boolean searchGroupEntriesPaged(LdapContext ctx, String baseDN, String filter, String[] returningAttrs,
                                           int pageSize, SearchResultHandler handler)
            throws NamingException, IOException {
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        searchControls.setReturningAttributes(returningAttrs);

        boolean paged = pageSize > 0;
        try {
            byte[] cookie = null;
            int page = 0;
            do {
                if (paged) {
                    ctx.setRequestControls(
                            new Control[] {new PagedResultsControl(pageSize, cookie, Control.NONCRITICAL)});
                }

                NamingEnumeration<SearchResult> results = null;
                try {
                    results = ctx.search(baseDN, filter, searchControls);
                    while (results.hasMore()) {
                        handler.handle(results.next());
                    }
                    // Read the response controls before closing: the cookie is only available once the
                    // enumeration for this page has been drained.
                    cookie = paged ? extractPagedResultsCookie(ctx.getResponseControls()) : null;
                } catch (PartialResultException e) {
                    // A referral the provider will not chase. With the domain root as base DN this is routine
                    // on Active Directory (CN=Configuration and the other subordinate naming contexts) and it
                    // surfaces after the real entries, so it must not fail an otherwise healthy refresh.
                    //
                    // But it also aborts this page's enumeration. If further pages were still pending we have
                    // genuinely lost group entries, and reporting that as a complete read would publish a
                    // partial map - which is indistinguishable from "these users are in no groups".
                    LOG.warn("LDAP group search hit an unfollowable referral on page {} for filter '{}'",
                            page + 1, filter, e);
                    return !hasFurtherPage(ctx, paged, filter);
                } finally {
                    closeNamingEnumeration(results);
                }

                if (++page >= MAX_SEARCH_PAGES && cookie != null) {
                    throw new NamingException("LDAP paged group search exceeded MAX_SEARCH_PAGES ("
                            + MAX_SEARCH_PAGES + ") for filter '" + filter + "'");
                }
            } while (cookie != null);
            return true;
        } finally {
            if (paged) {
                ctx.setRequestControls(null);
            }
        }
    }

    /**
     * Whether the page we just aborted still had a continuation cookie, i.e. whether entries were lost.
     * An unreadable response control means we cannot tell, which counts as lost - the same fail-closed
     * stance {@link #isDeterministicAbsence} takes.
     */
    private static boolean hasFurtherPage(LdapContext ctx, boolean paged, String filter) {
        if (!paged) {
            return false;
        }
        try {
            return extractPagedResultsCookie(ctx.getResponseControls()) != null;
        } catch (NamingException ne) {
            LOG.warn("Cannot tell whether more LDAP pages were pending after a referral for filter '{}', "
                    + "treating the search as incomplete", filter, ne);
            return true;
        }
    }

    /**
     * Pull the continuation cookie out of the paged results response. Returns null when the server sent no
     * such control (it does not support RFC 2696) or when the cookie is empty (the result set is complete).
     */
    @VisibleForTesting
    static byte[] extractPagedResultsCookie(Control[] responseControls) {
        if (responseControls == null) {
            return null;
        }
        for (Control control : responseControls) {
            if (control instanceof PagedResultsResponseControl) {
                byte[] cookie = ((PagedResultsResponseControl) control).getCookie();
                if (cookie != null && cookie.length > 0) {
                    return cookie;
                }
            }
        }
        return null;
    }

    /**
     * Consume the member values present in {@code attrs}, handling both the canonical attribute
     * ({@code member}) and the range-encoded form Active Directory uses for large groups
     * ({@code member;range=0-1499}) - see MS-ADTS 3.1.1.3.1.3.3.
     *
     * @return the offset the next range page starts at, or -1 when nothing further needs fetching.
     *         A missing member attribute also yields -1: an empty group is a determinate answer.
     */
    @VisibleForTesting
    static long consumeMemberPage(Attributes attrs, String memberAttrName, Consumer<String> memberConsumer)
            throws NamingException {
        String attrId = findMemberAttributeId(attrs, memberAttrName);
        if (attrId == null) {
            LOG.warn("LDAP group member attribute '{}' not found in attributes: {}", memberAttrName, attrs);
            return -1L;
        }

        Attribute attr = attrs.get(attrId);
        if (attr != null) {
            NamingEnumeration<?> e = attr.getAll();
            try {
                while (e.hasMore()) {
                    memberConsumer.accept((String) e.next());
                }
            } finally {
                closeNamingEnumeration(e);
            }
        }

        RangeInfo range = parseRangeSuffix(attrId);
        if (range == null || range.isTerminal()) {
            return -1L;
        }
        return range.end + 1;
    }

    /**
     * Request follow-up range pages ({@code member;range=N-*}) until the server answers with a terminal
     * ({@code -*}) marker.
     *
     * @return true when the terminal page was reached. false means the member list is incomplete -
     *         the caller must treat that as a failed refresh rather than publishing a truncated group.
     */
    @VisibleForTesting
    static boolean fetchRemainingMemberPages(DirContext ctx, String groupDN, String memberAttrName,
                                             long nextStart, Consumer<String> memberConsumer) {
        if (Strings.isNullOrEmpty(groupDN)) {
            LOG.warn("Cannot fetch range retrieval pages without a group DN");
            return false;
        }

        long start = nextStart;
        for (int page = 0; page < MAX_RANGE_PAGES; page++) {
            String nextAttrName = memberAttrName + ";range=" + start + "-*";
            Attributes attrs;
            try {
                attrs = ctx.getAttributes(groupDN, new String[] {nextAttrName});
            } catch (NamingException ne) {
                LOG.warn("Failed to fetch range page '{}' for LDAP group '{}', member list is incomplete",
                        nextAttrName, groupDN, ne);
                return false;
            }

            long following;
            try {
                // consumeMemberPage answers -1 both for a terminal page and for a response carrying no
                // member attribute at all. Probe first so the two cases stay distinguishable: the latter
                // means the server ignored our request and the member list is incomplete.
                if (findMemberAttributeId(attrs, memberAttrName) == null) {
                    LOG.warn("LDAP server returned no member attribute for range page '{}' of group '{}'",
                            nextAttrName, groupDN);
                    return false;
                }
                following = consumeMemberPage(attrs, memberAttrName, memberConsumer);
            } catch (NamingException ne) {
                LOG.warn("Failed to read range page '{}' for LDAP group '{}', member list is incomplete",
                        nextAttrName, groupDN, ne);
                return false;
            }

            if (following < 0) {
                return true;
            }

            if (following <= start) {
                LOG.warn("LDAP server did not advance range retrieval for group '{}' (asked for {}, got {})",
                        groupDN, start, following);
                return false;
            }
            start = following;
        }

        LOG.warn("LDAP group '{}' exceeded MAX_RANGE_PAGES ({}), member list is incomplete", groupDN, MAX_RANGE_PAGES);
        return false;
    }

    /**
     * Locate the attribute id actually used for the member attribute. Active Directory returns the
     * canonical name for small groups and a range-encoded id for large ones, and it may echo an empty
     * copy of either alongside the one that carries the values - so prefer whichever holds data.
     * Matching is case-insensitive because the LDAP attribute namespace is.
     */
    @VisibleForTesting
    static String findMemberAttributeId(Attributes attrs, String memberAttrName) throws NamingException {
        if (attrs == null) {
            return null;
        }
        String plainAttrId = null;
        String plainAttrIdWithValue = null;
        String rangeAttrId = null;
        NamingEnumeration<String> ids = attrs.getIDs();
        try {
            while (ids.hasMore()) {
                String id = ids.next();
                boolean isPlainMemberAttr = id.equalsIgnoreCase(memberAttrName);
                boolean isRangeMemberAttr = isMemberRangeAttribute(id, memberAttrName);
                if (!isPlainMemberAttr && !isRangeMemberAttr) {
                    continue;
                }

                Attribute attr = attrs.get(id);
                boolean hasValue = attr != null && attr.size() > 0;
                if (isRangeMemberAttr) {
                    if (hasValue) {
                        return id;
                    }
                    if (rangeAttrId == null) {
                        rangeAttrId = id;
                    }
                } else if (hasValue) {
                    if (plainAttrIdWithValue == null) {
                        plainAttrIdWithValue = id;
                    }
                } else if (plainAttrId == null) {
                    plainAttrId = id;
                }
            }
        } finally {
            closeNamingEnumeration(ids);
        }
        if (plainAttrIdWithValue != null) {
            return plainAttrIdWithValue;
        }
        if (rangeAttrId != null) {
            return rangeAttrId;
        }
        return plainAttrId;
    }

    private static boolean isMemberRangeAttribute(String attrId, String memberAttrName) {
        int semi = attrId.indexOf(';');
        return semi > 0
                && attrId.substring(0, semi).equalsIgnoreCase(memberAttrName)
                && parseRangeSuffix(attrId) != null;
    }

    /**
     * Parse the range suffix of an attribute id such as {@code member;range=0-1499} or
     * {@code member;range=1500-*}. Returns null when the id carries no valid range suffix.
     */
    @VisibleForTesting
    static RangeInfo parseRangeSuffix(String attrId) {
        if (attrId == null) {
            return null;
        }
        Matcher m = RANGE_PATTERN.matcher(attrId);
        if (!m.find()) {
            return null;
        }
        String endStr = m.group(2);
        long end = "*".equals(endStr) ? -1L : Long.parseLong(endStr);
        return new RangeInfo(Long.parseLong(m.group(1)), end);
    }

    @VisibleForTesting
    static final class RangeInfo {
        final long start;
        final long end; // -1 stands for "*", i.e. the terminal page.

        RangeInfo(long start, long end) {
            this.start = start;
            this.end = end;
        }

        boolean isTerminal() {
            return end == -1L;
        }
    }

    /** A group whose member list was cut short by range retrieval, to be finished in Phase B. */
    private static final class PendingMemberRange {
        final String groupDN;
        final String groupName;
        final long nextStart;

        PendingMemberRange(String groupDN, String groupName, long nextStart) {
            this.groupDN = groupDN;
            this.groupName = groupName;
            this.nextStart = nextStart;
        }
    }

    private static void collectMember(Map<String, Set<String>> groups,
                                      String groupName,
                                      String memberDN,
                                      UserNameExtractInterface userNameExtractInterface) {
        String extractUserName = userNameExtractInterface.extract(memberDN);

        if (extractUserName == null) {
            LOG.debug("Failed to extract user name from member DN: '{}'", memberDN);
            return;
        }

        // Normalize extracted username for case-insensitive matching
        // LDAP is case-insensitive by default, so we normalize to ensure consistent mapping
        String normalizedUserName = LDAPAuthProvider.normalizeUsername(extractUserName);

        groups.computeIfAbsent(normalizedUserName, k -> ConcurrentHashMap.newKeySet()).add(groupName);

        LOG.debug("Successfully extracted user '{}' from member '{}', added to group '{}'",
                extractUserName, memberDN, groupName);
    }

    /**
     * Extract the fully-qualified DN of a group from a SearchResult. getNameInNamespace() is preferred
     * because it returns the absolute DN in normalized form, which is what a follow-up getAttributes()
     * needs when the member attribute is split across range retrieval pages.
     */
    @VisibleForTesting
    static String extractGroupDN(SearchResult result, String baseDN) {
        try {
            String dn = result.getNameInNamespace();
            if (dn != null && !dn.isEmpty()) {
                return dn;
            }
        } catch (UnsupportedOperationException | IllegalStateException ignored) {
            // Not every LDAP provider implements getNameInNamespace; fall through to getName().
        }
        String name = result.getName();
        if (name == null || name.isEmpty() || !result.isRelative()) {
            return name;
        }
        return qualifyRelativeDN(name, baseDN);
    }

    @VisibleForTesting
    static String qualifyRelativeDN(String dn, String baseDN) {
        if (Strings.isNullOrEmpty(dn) || Strings.isNullOrEmpty(baseDN)) {
            return dn;
        }
        String normalizedDN = StringUtils.strip(dn.trim(), "\"'");
        String normalizedBaseDN = StringUtils.strip(baseDN.trim(), "\"'");
        if (normalizedDN.isEmpty() || normalizedBaseDN.isEmpty()) {
            return normalizedDN;
        }
        String lowerDN = normalizedDN.toLowerCase(Locale.ROOT);
        String lowerBaseDN = normalizedBaseDN.toLowerCase(Locale.ROOT);
        if (lowerDN.equals(lowerBaseDN) || lowerDN.endsWith("," + lowerBaseDN)) {
            return normalizedDN;
        }
        return normalizedDN + "," + normalizedBaseDN;
    }

    private static void closeNamingEnumeration(NamingEnumeration<?> enumeration) {
        if (enumeration != null) {
            try {
                enumeration.close();
            } catch (NamingException ne) {
                LOG.debug("Failed to close LDAP naming enumeration", ne);
            }
        }
    }

    private static void closeDirContext(DirContext ctx) {
        if (ctx != null) {
            try {
                ctx.close();
            } catch (NamingException ne) {
                LOG.warn("Failed to close LDAP DirContext", ne);
            }
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

    // Returns LdapContext rather than DirContext so the paged results control can be attached.
    // LdapContext extends DirContext, so this stays source-compatible for every caller.
    public LdapContext createDirContextOnConnection(String dn, String pwd) throws NamingException, IOException,
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

        return new InitialLdapContext(environment, null);
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
