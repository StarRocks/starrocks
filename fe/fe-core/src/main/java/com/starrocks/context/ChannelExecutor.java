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

package com.starrocks.context;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Multi-agent collaboration primitives. Channel messages are persisted as ordinary entities in a
 * channel-typed collection via {@link ContextWriteExecutor}; subscriptions are persisted in the
 * pattern-aware {@link ContextInternalTables#CHANNEL_SUBSCRIPTIONS} table.
 *
 * <p>{@code pull(...)} has two modes:
 * <ul>
 *     <li>subscriber-scoped pull when {@code subscriber} is provided: active subscriptions for the
 *     subscriber/channel scope are loaded, rows are filtered by pattern against preview/body, and
 *     each matching subscription cursor is advanced in-place.</li>
 *     <li>raw collection pull when {@code subscriber} is omitted: preserves the previous
 *     collection-scan behavior for compatibility.</li>
 * </ul>
 */
public class ChannelExecutor {

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String DEFAULT_SUBSCRIPTION_PATTERN = ".*";
    private static final String DEFAULT_SUBSCRIPTION_TYPE = "all";
    private static final int DEFAULT_PULL_LIMIT = 100;
    private static final int SUBSCRIBER_PULL_BATCH_MIN = 100;
    private static final int SUBSCRIBER_PULL_BATCH_MAX = 1000;
    // Subscriber pull walks pages of channel rows until it fills `limit` matches or exhausts the
    // collection. Without an iteration cap, a sparse-match subscription on a busy channel could
    // scan the whole tail in one HTTP round; cap the pages so the puller hands control back to
    // the client and the next long-poll request resumes from the new cursor.
    private static final int MAX_PULL_PAGES = 5;
    private static final int MAX_SUBSCRIPTION_PATTERN_LENGTH = 256;
    // Reject regex features that are disproportionately expensive or difficult to bound under the
    // Java backtracking engine. Subscriptions are evaluated on every long-poll wake, so a single
    // catastrophic pattern can pin a worker.
    private static final Pattern UNSAFE_REGEX_TOKEN = Pattern.compile(
            "\\\\[1-9]|\\(\\?<[-=!]|\\(\\?[=!]|\\(\\?>");
    // Class-level Pattern cache. Subscription records are rebuilt on every loadActiveSubscriptions
    // call (every long-poll wake), so without this every pull recompiles every subscriber's regex.
    // Keyed by raw pattern string; Optional.empty() memoizes "this regex failed to compile" so
    // bad patterns aren't recompiled either.
    private static final Cache<String, Optional<Pattern>> PATTERN_CACHE = Caffeine.newBuilder()
            .maximumSize(2048)
            .expireAfterAccess(Duration.ofMinutes(30))
            .build();

    private final ContextMgr contextMgr;
    private final ContextWriteExecutor writeExecutor;
    private final ChannelWaitRegistry waitRegistry;

    public ChannelExecutor(ContextMgr contextMgr, ContextWriteExecutor writeExecutor) {
        this(contextMgr, writeExecutor, new ChannelWaitRegistry());
    }

    ChannelExecutor(ContextMgr contextMgr, ContextWriteExecutor writeExecutor, ChannelWaitRegistry waitRegistry) {
        this.contextMgr = contextMgr;
        this.writeExecutor = writeExecutor;
        this.waitRegistry = waitRegistry;
    }

    public void subscribe(String subscriber, String contextBase, String collection,
                          String pattern, String subscriptionType) {
        ensureCollectionIsChannel(contextBase, collection);
        long cbId = requireContextBaseId(contextBase);
        long colId = requireCollectionId(contextBase, collection);
        String normalizedSubscriber = requireSubscriber(subscriber);
        String normalizedPattern = normalizePattern(pattern);
        validateUserPattern(normalizedPattern);
        String effectiveType = normalizeSubscriptionType(subscriptionType);

        String subscriptionTable = ContextInternalTables.CHANNEL_SUBSCRIPTIONS;
        List<SubscriptionRecord> existing = loadActiveSubscriptions(
                subscriptionTable, normalizedSubscriber, cbId, colId, normalizedPattern);
        if (!existing.isEmpty()) {
            for (SubscriptionRecord record : existing) {
                if (!effectiveType.equals(record.subscriptionType)) {
                    upsertSubscription(subscriptionTable, record.subscriptionId, record.subscriber,
                            record.contextBaseId, record.collectionId, record.pattern, effectiveType,
                            record.lastCursorSnapshot, record.createdTime, false);
                }
            }
            return;
        }

        upsertSubscription(subscriptionTable, GlobalStateMgr.getCurrentState().getNextId(),
                normalizedSubscriber, cbId, colId, normalizedPattern, effectiveType,
                null, TS_FMT.format(LocalDateTime.now()), false);
    }

    public void unsubscribe(String subscriber, String contextBase, String collection, String pattern) {
        ensureCollectionIsChannel(contextBase, collection);
        long cbId = requireContextBaseId(contextBase);
        long colId = requireCollectionId(contextBase, collection);
        String normalizedSubscriber = requireSubscriber(subscriber);
        String normalizedPattern = normalizePattern(pattern);
        String subscriptionTable = ContextInternalTables.CHANNEL_SUBSCRIPTIONS;
        List<SubscriptionRecord> existing = loadActiveSubscriptions(
                subscriptionTable, normalizedSubscriber, cbId, colId, normalizedPattern);
        for (SubscriptionRecord record : existing) {
            upsertSubscription(subscriptionTable, record.subscriptionId, record.subscriber,
                    record.contextBaseId, record.collectionId, record.pattern, record.subscriptionType,
                    record.lastCursorSnapshot, record.createdTime, true);
        }
    }

    /**
     * Send a message to a channel collection. Messages are ordinary {@code page} entities so they
     * flow through the normal versioning/retrieval stack.
     */
    public ContextWriteExecutor.UpsertResult send(String contextBase, String collection,
                                                  String author, String content, String title) {
        MetricRepo.COUNTER_CONTEXT_CHANNEL_SEND_TOTAL.increase(1L);
        ensureCollectionIsChannel(contextBase, collection);
        ContextCollectionName name = new ContextCollectionName(contextBase, collection, NodePosition.ZERO);
        Map<String, Expr> entityArgs = new LinkedHashMap<>();
        entityArgs.put("entity_type", new StringLiteral("page"));
        entityArgs.put("title", new StringLiteral(Strings.nullToEmpty(title)));
        entityArgs.put("content", new StringLiteral(Strings.nullToEmpty(content)));
        if (!Strings.isNullOrEmpty(author)) {
            entityArgs.put("entity_key", new StringLiteral(
                    "channel-msg-" + author + "-" + System.currentTimeMillis()));
        }
        Map<String, Expr> options = new LinkedHashMap<>();
        ContextWriteExecutor.UpsertResult result = writeExecutor.upsert(name, entityArgs, options);
        waitRegistry.signal(channelKey(contextBase, collection));
        return result;
    }

    /**
     * Pull messages from a channel collection. With a subscriber, only rows newer than each active
     * subscription cursor and matching at least one active pattern are returned; without a
     * subscriber, the previous raw collection scan is preserved as a compatibility fallback.
     */
    public PullResult pull(String contextBase, String collection, String subscriber, Long afterSnapshot, int limit) {
        return pull(contextBase, collection, subscriber, afterSnapshot, limit, 0L);
    }

    /**
     * Pull messages from a channel collection, optionally long-polling until a sender wakes the
     * channel or {@code waitTimeoutMs} elapses.
     */
    public PullResult pull(String contextBase, String collection, String subscriber,
                           Long afterSnapshot, int limit, long waitTimeoutMs) {
        MetricRepo.COUNTER_CONTEXT_CHANNEL_PULL_TOTAL.increase(1L);
        ensureCollectionIsChannel(contextBase, collection);
        long cbId = requireContextBaseId(contextBase);
        long colId = requireCollectionId(contextBase, collection);
        int effectiveLimit = limit > 0 ? limit : DEFAULT_PULL_LIMIT;
        long observedGeneration = waitRegistry.currentGeneration(channelKey(contextBase, collection));
        PullResult immediate = Strings.isNullOrEmpty(subscriber)
                ? new PullResult(runQueryRaw(buildRawPullSql(colId,
                        afterSnapshot == null ? 0L : afterSnapshot, effectiveLimit)), null, Collections.emptyList())
                : pullForSubscriber(ContextInternalTables.CHANNEL_SUBSCRIPTIONS, requireSubscriber(subscriber), cbId, colId,
                        afterSnapshot, effectiveLimit);
        if (immediate.rows.size() > 0 || waitTimeoutMs <= 0L) {
            return immediate;
        }

        try {
            if (!waitRegistry.awaitChange(channelKey(contextBase, collection), observedGeneration, waitTimeoutMs)) {
                return immediate;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return immediate;
        }
        return Strings.isNullOrEmpty(subscriber)
                ? new PullResult(runQueryRaw(buildRawPullSql(colId,
                        afterSnapshot == null ? 0L : afterSnapshot, effectiveLimit)), null, Collections.emptyList())
                : pullForSubscriber(ContextInternalTables.CHANNEL_SUBSCRIPTIONS, requireSubscriber(subscriber), cbId, colId,
                        afterSnapshot, effectiveLimit);
    }

    private PullResult pullForSubscriber(String subscriptionTable, String subscriber,
                                         long contextBaseId, long collectionId,
                                         Long afterSnapshot, int limit) {
        List<SubscriptionRecord> subscriptions = loadActiveSubscriptions(
                subscriptionTable, subscriber, contextBaseId, collectionId, null);
        if (subscriptions.isEmpty()) {
            return new PullResult(new JsonArray(), subscriber, Collections.emptyList());
        }

        LinkedHashSet<String> patterns = new LinkedHashSet<>();
        long scanCursor = Long.MAX_VALUE;
        for (SubscriptionRecord subscription : subscriptions) {
            scanCursor = Math.min(scanCursor, subscription.preparePull(afterSnapshot));
            patterns.add(subscription.pattern);
        }
        if (scanCursor == Long.MAX_VALUE) {
            scanCursor = afterSnapshot == null ? 0L : afterSnapshot;
        }

        JsonArray matchedRows = new JsonArray();
        LinkedHashSet<Long> emittedEntityIds = new LinkedHashSet<>();
        int batchSize = computeSubscriberPullBatchSize(limit, subscriptions.size());
        boolean exhausted = false;
        int pagesScanned = 0;
        while (matchedRows.size() < limit && !exhausted && pagesScanned < MAX_PULL_PAGES) {
            JsonArray page = runQueryRaw(buildSubscriberPullSql(collectionId, scanCursor, batchSize));
            pagesScanned++;
            if (page.size() == 0) {
                break;
            }

            long lastPageSnapshot = scanCursor;
            for (JsonElement row : page) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data == null || data.size() < 7) {
                    continue;
                }

                long entityId = data.get(0).getAsLong();
                long snapshot = data.get(4).isJsonNull() ? 0L : data.get(4).getAsLong();
                lastPageSnapshot = snapshot;
                String preview = jsonString(data, 3);
                String body = jsonString(data, 6);

                // Single-pass match-and-advance. Previously this loop ran shouldDeliver in a first
                // pass and advanceCursor in a second pass over every subscription — for a busy
                // channel with many subscribers, every row paid 2*S regex matches. Folding into
                // one pass halves the regex work; advanceCursor is still called for every
                // subscription regardless of match (it only updates if snapshot > cursor).
                boolean matchedAny = false;
                for (SubscriptionRecord subscription : subscriptions) {
                    if (subscription.shouldDeliver(snapshot, preview, body)) {
                        matchedAny = true;
                    }
                    subscription.advanceCursor(snapshot);
                }
                if (!matchedAny) {
                    continue;
                }
                if (emittedEntityIds.add(entityId)) {
                    matchedRows.add(publicPullRow(data));
                    if (matchedRows.size() >= limit) {
                        break;
                    }
                }
            }

            if (matchedRows.size() >= limit) {
                break;
            }
            if (page.size() < batchSize || lastPageSnapshot <= scanCursor) {
                exhausted = true;
            } else {
                scanCursor = lastPageSnapshot;
            }
        }

        persistUpdatedCursors(subscriptionTable, subscriptions);
        return new PullResult(matchedRows, subscriber, new ArrayList<>(patterns));
    }

    private void persistUpdatedCursors(String subscriptionTable, List<SubscriptionRecord> subscriptions) {
        for (SubscriptionRecord subscription : subscriptions) {
            if (!subscription.cursorChanged()) {
                continue;
            }
            upsertSubscription(subscriptionTable, subscription.subscriptionId, subscription.subscriber,
                    subscription.contextBaseId, subscription.collectionId, subscription.pattern,
                    subscription.subscriptionType, subscription.cursorSnapshot,
                    subscription.createdTime, false);
        }
    }

    private List<SubscriptionRecord> loadActiveSubscriptions(String subscriptionTable, String subscriber,
                                                             long contextBaseId,
                                                             long collectionId, String pattern) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT subscription_id, subscriber, contextbase_id, collection_id, pattern, ")
                .append("subscription_type, last_cursor_snapshot, created_time, deleted ")
                .append("FROM ").append(ContextInternalTables.DATABASE).append('.')
                .append(subscriptionTable)
                .append(" WHERE subscriber = '").append(escapeSql(subscriber)).append('\'')
                .append(" AND contextbase_id = ").append(contextBaseId)
                .append(" AND collection_id = ").append(collectionId)
                .append(" AND deleted = false");
        if (!Strings.isNullOrEmpty(pattern)) {
            sql.append(" AND pattern = '").append(escapeSql(pattern)).append('\'');
        }
        sql.append(" ORDER BY subscription_id ASC");

        JsonArray rows = runQueryRaw(sql.toString());
        List<SubscriptionRecord> subscriptions = new ArrayList<>();
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data == null || data.size() < 9) {
                continue;
            }
            subscriptions.add(new SubscriptionRecord(
                    data.get(0).getAsLong(),
                    data.get(1).isJsonNull() ? subscriber : data.get(1).getAsString(),
                    data.get(2).isJsonNull() ? contextBaseId : data.get(2).getAsLong(),
                    data.get(3).isJsonNull() ? collectionId : data.get(3).getAsLong(),
                    data.get(4).isJsonNull() ? DEFAULT_SUBSCRIPTION_PATTERN : data.get(4).getAsString(),
                    data.get(5).isJsonNull() ? DEFAULT_SUBSCRIPTION_TYPE : data.get(5).getAsString(),
                    data.get(6).isJsonNull() ? null : data.get(6).getAsLong(),
                    data.get(7).isJsonNull() ? TS_FMT.format(LocalDateTime.now()) : data.get(7).getAsString(),
                    ContextJsonUtil.parseBool(data.get(8))));
        }
        return subscriptions;
    }

    private void upsertSubscription(String subscriptionTable, long subscriptionId, String subscriber,
                                    long contextBaseId, long collectionId,
                                    String pattern, String subscriptionType, Long lastCursorSnapshot,
                                    String createdTime, boolean deleted) {
        String sql = String.format(
                "INSERT INTO %s.%s (subscription_id, subscriber, contextbase_id, collection_id, pattern, "
                        + "subscription_type, last_cursor_snapshot, created_time, deleted) "
                        + "VALUES (%d, '%s', %d, %d, '%s', '%s', %s, '%s', %b)",
                ContextInternalTables.DATABASE, subscriptionTable,
                subscriptionId, escapeSql(subscriber), contextBaseId, collectionId,
                escapeSql(pattern), escapeSql(subscriptionType), sqlLongOrNull(lastCursorSnapshot),
                escapeSql(createdTime), deleted);
        SimpleExecutor.getRepoExecutor().executeDML(sql);
    }

    private String buildRawPullSql(long collectionId, long afterSnapshot, int limit) {
        return String.format(
                "SELECT entity_id, current_version, entity_key, current_preview, current_snapshot_version "
                        + "FROM %s.%s WHERE collection_id = %d AND current_snapshot_version > %d "
                        + "AND current_deleted = false ORDER BY current_snapshot_version ASC LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                collectionId, afterSnapshot, limit);
    }

    private String buildSubscriberPullSql(long collectionId, long afterSnapshot, int limit) {
        return String.format(
                "SELECT h.entity_id, h.current_version, h.entity_key, h.current_preview, "
                        + "h.current_snapshot_version, v.title, v.body "
                        + "FROM %s.%s h JOIN %s.%s v "
                        + "ON h.entity_id = v.entity_id AND h.current_version = v.version "
                        + "WHERE h.collection_id = %d AND h.current_snapshot_version > %d "
                        + "AND h.current_deleted = false ORDER BY h.current_snapshot_version ASC LIMIT %d",
                ContextInternalTables.DATABASE, ContextInternalTables.HEADS,
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS,
                collectionId, afterSnapshot, limit);
    }

    private int computeSubscriberPullBatchSize(int limit, int subscriptionCount) {
        int scaled = limit * Math.max(4, subscriptionCount);
        if (scaled < SUBSCRIBER_PULL_BATCH_MIN) {
            return SUBSCRIBER_PULL_BATCH_MIN;
        }
        return Math.min(scaled, SUBSCRIBER_PULL_BATCH_MAX);
    }

    private JsonObject publicPullRow(JsonArray data) {
        JsonArray publicData = new JsonArray();
        for (int i = 0; i < 5 && i < data.size(); i++) {
            publicData.add(data.get(i).deepCopy());
        }
        JsonObject row = new JsonObject();
        row.add("data", publicData);
        return row;
    }

    private String jsonString(JsonArray data, int index) {
        if (data == null || index >= data.size() || data.get(index).isJsonNull()) {
            return "";
        }
        return data.get(index).getAsString();
    }

    private String requireSubscriber(String subscriber) {
        if (Strings.isNullOrEmpty(subscriber)) {
            throw new IllegalArgumentException("subscriber is required");
        }
        return subscriber;
    }

    private String normalizePattern(String pattern) {
        String trimmed = Strings.nullToEmpty(pattern).trim();
        return trimmed.isEmpty() ? DEFAULT_SUBSCRIPTION_PATTERN : trimmed;
    }

    static void validateUserPattern(String pattern) {
        if (pattern == null) {
            return;
        }
        if (pattern.length() > MAX_SUBSCRIPTION_PATTERN_LENGTH) {
            throw new IllegalArgumentException(
                    "pattern exceeds max length " + MAX_SUBSCRIPTION_PATTERN_LENGTH);
        }
        if (!isSafeRegexPattern(pattern)) {
            throw new IllegalArgumentException("pattern uses an unsafe regex construct");
        }
    }

    static boolean isSafeRegexPattern(String pattern) {
        if (pattern == null || DEFAULT_SUBSCRIPTION_PATTERN.equals(pattern)) {
            return true;
        }
        if (pattern.length() > MAX_SUBSCRIPTION_PATTERN_LENGTH) {
            return false;
        }
        if (UNSAFE_REGEX_TOKEN.matcher(pattern).find()) {
            return false;
        }
        return !hasNestedQuantifiedGroup(pattern);
    }

    private static boolean hasNestedQuantifiedGroup(String pattern) {
        ArrayDeque<Boolean> groupHasComplexity = new ArrayDeque<>();
        boolean escaped = false;
        boolean inCharClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (inCharClass) {
                if (c == ']') {
                    inCharClass = false;
                }
                continue;
            }
            if (c == '[') {
                inCharClass = true;
                continue;
            }
            if (c == '(') {
                groupHasComplexity.push(false);
                continue;
            }
            if (c == ')') {
                if (groupHasComplexity.isEmpty()) {
                    continue;
                }
                boolean innerHasComplexity = groupHasComplexity.pop();
                int nextIndex = i + 1;
                if (nextIndex < pattern.length() && isQuantifierStart(pattern.charAt(nextIndex))) {
                    if (innerHasComplexity) {
                        return true;
                    }
                    markCurrentGroupComplex(groupHasComplexity);
                }
                continue;
            }
            if ((c == '|' || isQuantifierStart(c)) && !groupHasComplexity.isEmpty()) {
                markCurrentGroupComplex(groupHasComplexity);
            }
        }
        return false;
    }

    private static void markCurrentGroupComplex(ArrayDeque<Boolean> groupHasComplexity) {
        if (groupHasComplexity.isEmpty()) {
            return;
        }
        groupHasComplexity.pop();
        groupHasComplexity.push(true);
    }

    private static boolean isQuantifierStart(char c) {
        return c == '*' || c == '+' || c == '?' || c == '{';
    }

    private String normalizeSubscriptionType(String subscriptionType) {
        String trimmed = Strings.nullToEmpty(subscriptionType).trim();
        return trimmed.isEmpty() ? DEFAULT_SUBSCRIPTION_TYPE : trimmed;
    }

    private void ensureCollectionIsChannel(String contextBase, String collection) {
        ContextMgr.CollectionMeta col = findCollection(contextBase, collection);
        if (col == null) {
            throw new IllegalStateException("collection not found: " + contextBase + "." + collection);
        }
        if (!"channel".equalsIgnoreCase(col.getCollectionType())) {
            throw new IllegalStateException(
                    "collection " + contextBase + "." + collection + " is not a channel (type="
                            + col.getCollectionType() + ")");
        }
    }

    private long requireContextBaseId(String contextBase) {
        ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(contextBase);
        if (cb == null) {
            throw new IllegalStateException("contextbase not found: " + contextBase);
        }
        return cb.getId();
    }

    private long requireCollectionId(String contextBase, String collection) {
        ContextMgr.CollectionMeta col = findCollection(contextBase, collection);
        if (col == null) {
            throw new IllegalStateException("collection not found: " + contextBase + "." + collection);
        }
        return col.getId();
    }

    private ContextMgr.CollectionMeta findCollection(String contextBase, String collection) {
        for (ContextMgr.CollectionMeta m : contextMgr.listCollections(contextBase)) {
            if (m.getName().equals(collection)) {
                return m;
            }
        }
        return null;
    }

    private JsonArray runQueryRaw(String sql) {
        return ContextSqlSupport.executeDql(sql);
    }

    private static String sqlLongOrNull(Long value) {
        return value == null ? "NULL" : String.valueOf(value);
    }

    private static String escapeSql(String s) {
        return ContextSqlEscape.body(s);
    }

    private String channelKey(String contextBase, String collection) {
        return contextBase + "." + collection;
    }

    public static final class PullResult {
        public final JsonArray rows;
        public final String subscriber;
        public final List<String> patterns;

        public PullResult(JsonArray rows, String subscriber, List<String> patterns) {
            this.rows = rows == null ? new JsonArray() : rows;
            this.subscriber = subscriber;
            this.patterns = patterns == null ? Collections.emptyList() : new ArrayList<>(patterns);
        }
    }

    private static final class SubscriptionRecord {
        private final long subscriptionId;
        private final String subscriber;
        private final long contextBaseId;
        private final long collectionId;
        private final String pattern;
        private final String subscriptionType;
        private final Long lastCursorSnapshot;
        private final String createdTime;
        private final boolean deleted;
        private final Pattern regexPattern;
        private long cursorSnapshot;

        private SubscriptionRecord(long subscriptionId, String subscriber, long contextBaseId, long collectionId,
                                   String pattern, String subscriptionType, Long lastCursorSnapshot,
                                   String createdTime, boolean deleted) {
            this.subscriptionId = subscriptionId;
            this.subscriber = subscriber;
            this.contextBaseId = contextBaseId;
            this.collectionId = collectionId;
            this.pattern = Strings.isNullOrEmpty(pattern) ? DEFAULT_SUBSCRIPTION_PATTERN : pattern;
            this.subscriptionType = Strings.isNullOrEmpty(subscriptionType)
                    ? DEFAULT_SUBSCRIPTION_TYPE : subscriptionType;
            this.lastCursorSnapshot = lastCursorSnapshot;
            this.createdTime = createdTime;
            this.deleted = deleted;
            this.regexPattern = compilePattern(this.pattern);
            this.cursorSnapshot = lastCursorSnapshot == null ? 0L : lastCursorSnapshot;
        }

        private long preparePull(Long afterSnapshotOverride) {
            long effectiveCursor = lastCursorSnapshot == null ? 0L : lastCursorSnapshot;
            if (afterSnapshotOverride != null) {
                effectiveCursor = Math.max(effectiveCursor, afterSnapshotOverride);
            }
            this.cursorSnapshot = effectiveCursor;
            return effectiveCursor;
        }

        private boolean shouldDeliver(long snapshot, String preview, String body) {
            return !deleted && snapshot > cursorSnapshot && matches(preview, body);
        }

        private void advanceCursor(long snapshot) {
            if (snapshot > cursorSnapshot) {
                cursorSnapshot = snapshot;
            }
        }

        private boolean cursorChanged() {
            long baseline = lastCursorSnapshot == null ? 0L : lastCursorSnapshot;
            return cursorSnapshot > baseline;
        }

        private boolean matches(String preview, String body) {
            if (DEFAULT_SUBSCRIPTION_PATTERN.equals(pattern)) {
                return true;
            }
            String haystackPreview = Strings.nullToEmpty(preview);
            String haystackBody = Strings.nullToEmpty(body);
            if (regexPattern != null) {
                return regexPattern.matcher(haystackPreview).find() || regexPattern.matcher(haystackBody).find();
            }
            return haystackPreview.contains(pattern) || haystackBody.contains(pattern);
        }

        private static Pattern compilePattern(String pattern) {
            if (DEFAULT_SUBSCRIPTION_PATTERN.equals(pattern)) {
                return null;
            }
            return PATTERN_CACHE.get(pattern, p -> {
                if (!isSafeRegexPattern(p)) {
                    return Optional.empty();
                }
                try {
                    return Optional.of(Pattern.compile(p));
                } catch (PatternSyntaxException e) {
                    return Optional.empty();
                }
            }).orElse(null);
        }
    }
}
