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

package com.starrocks.context.retrieval;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.starrocks.context.ContextInternalTables;
import com.starrocks.context.ContextSqlEscape;
import com.starrocks.context.ContextSqlSupport;
import com.starrocks.context.markdown.MarkdownExtractor;
import com.starrocks.context.retrieval.rerank.support.Bm25;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Text search over {@link ContextInternalTables#FRAGMENTS}. The architecture doc §10.3 specifies
 * that text retrieval runs on fragment rows (not on {@code versions.body}) so snippets keep the
 * line-range metadata needed for deep disclosure.
 *
 * <p>The {@code fragment_text} column carries an inline {@code USING GIN ("parser"="english")}
 * inverted index; this executor pushes down to it via {@code MATCH "kw"} when the request is a
 * plain single-token keyword. For patterns that contain whitespace, SQL-LIKE wildcards
 * ({@code %} / {@code _}), or other non-word characters, it falls back to a {@code LIKE
 * '%pattern%'} substring scan — that path is functionally correct but not index-pushdown.
 * Entity folding (one result per entity, even when multiple fragments match) happens in a SQL
 * window-function fold so the caller sees the canonical "entity is the unit of retrieval" contract
 * from the API design doc §7.5.
 *
 * <p>An exact-BM25 variant that reads term postings via the {@code gin_term_postings} table
 * function (for corpus-wide IDF) is deferred: that TVF is a BE feature that is not registered yet,
 * so it will land together with the BE work. Today every query uses the MATCH/LIKE path here, which
 * scores with a BM25-lite TF-saturation + length-normalization fold ({@link Bm25#scoreSql}).
 */
public class TextSearchExecutor {

    private static final Logger LOG = LogManager.getLogger(TextSearchExecutor.class);

    // A pattern is "MATCH-friendly" when it's a single non-empty token of word characters only.
    // Anything with whitespace, LIKE wildcards (% / _), or other special characters falls back to
    // the LIKE substring path so semantics don't silently change for substring queries.
    private static final java.util.regex.Pattern MATCH_FRIENDLY =
            java.util.regex.Pattern.compile("^[A-Za-z0-9_\\-]+$");

    /**
     * One fragment-level hit, before entity folding.
     */
    public static final class FragmentHit {
        public final long entityId;
        public final long version;
        public final long fragmentId;
        public final String fragmentKind;
        public final int lineStart;
        public final int lineEnd;
        public final String snippet;

        public FragmentHit(long entityId, long version, long fragmentId, String fragmentKind,
                           int lineStart, int lineEnd, String snippet) {
            this.entityId = entityId;
            this.version = version;
            this.fragmentId = fragmentId;
            this.fragmentKind = fragmentKind;
            this.lineStart = lineStart;
            this.lineEnd = lineEnd;
            this.snippet = snippet;
        }
    }

    /**
     * Entity-level folded result. {@code topSnippet} is the single best snippet the caller should
     * surface; {@code hitCount} gives the overall match intensity for ranking.
     */
    public static final class EntityHit {
        public final long entityId;
        public final int hitCount;
        public final FragmentHit topSnippet;
        public final double textScore;

        public EntityHit(long entityId, int hitCount, FragmentHit topSnippet, double textScore) {
            this.entityId = entityId;
            this.hitCount = hitCount;
            this.topSnippet = topSnippet;
            this.textScore = textScore;
        }
    }

    public static final class Request {
        public String pattern;
        // Correlation id stitching the text-search line to the enclosing context_search line (set by
        // ContextSearchExecutor). When null, this executor mints its own for standalone calls.
        public String requestId;
        public Long contextBaseId;
        // Multi-contextbase scope. Used only when contextBaseId is null (single value wins). Mirrors
        // the collectionId / collectionIds pattern: filters heads.contextbase_id IN (...).
        public List<Long> contextBaseIds;
        public Long collectionId;
        public List<Long> collectionIds;
        public String entityType;
        public int maxFragmentScan = 2000;
        public int maxResults = 50;
        public int offset = 0;
        // Snapshot fence per architecture doc §10.3 step 3 — when set the executor uses an as-of
        // view of fragments instead of the current heads. -1 means current.
        public long snapshotFence = -1L;
        // Lower bound on entity.confidence — fragments belonging to entities below this threshold
        // are filtered server-side. Spec: API doc §7.5 / arch doc §10.3 step 1.
        public Double confidenceMin;
        // Grep-style options. The flags follow GNU grep semantics:
        //   -i  caseInsensitive — match regardless of case
        //   -n  includeLineNumbers — return fragment line ranges in the snippet (already on by default)
        //   -c  countOnly — return only hit counts per entity, no snippets
        //   -l  filenamesOnly — return only entity ids of matching entities, no snippets
        //   -A  afterLines / -B beforeLines / -C contextLines — snippet expansion (currently
        //       echoed in explain; the actual snippet text comes from fragment_text today)
        public boolean caseInsensitive;
        public boolean countOnly;
        public boolean filenamesOnly;
        public Integer afterLines;
        public Integer beforeLines;
        public Integer contextLines;
    }

    public List<EntityHit> search(Request request) {
        // Reject null / empty / whitespace-only patterns. Strings.isNullOrEmpty alone would let a
        // whitespace-only pattern through: it trims to empty, produces no tokens, and the fallback
        // would build `LIKE '%%'` — a whole-corpus scan returning an arbitrary first page. A blank
        // search must behave like an empty query.
        if (request.pattern == null || request.pattern.trim().isEmpty()) {
            return new ArrayList<>();
        }
        return runTextSearch(request);
    }

    /**
     * MATCH/LIKE text search over the fragment_text GIN index, with a SQL window-function entity
     * fold. Three query modes:
     *   (a) caseInsensitive: LIKE on LOWER(fragment_text) with the trimmed phrase.
     *   (b) single token, case-sensitive: MATCH push-down to the GIN inverted index.
     *   (c) multi-token, case-sensitive: per-token OR of LIKE substrings (MATCH can't be OR'd).
     */
    private List<EntityHit> runTextSearch(Request request) {
        StringBuilder where = new StringBuilder();
        String trimmed = request.pattern.trim();
        // Tokenize the request on whitespace + punctuation. The MATCH index path is per-token
        // (the inline `parser=english` analyzer indexes individual word tokens, lowercased), so
        // a multi-word query has to be turned into an OR of per-token MATCH predicates rather
        // than a single MATCH over the raw phrase — otherwise the index lookup is for one token
        // that doesn't exist (e.g. "smb closed won baseline" as a single token) and the search
        // returns zero hits even when the source text contains every word.
        //
        // The fallback LIKE path likewise has to OR each token's substring scan; a single LIKE
        // over the full phrase only fires when the markdown contains that exact phrase.
        String[] tokens = trimmed.split("[\\s\\p{Punct}]+");
        java.util.List<String> validTokens = new java.util.ArrayList<>();
        for (String t : tokens) {
            if (!t.isEmpty() && MATCH_FRIENDLY.matcher(t).matches()) {
                validTokens.add(t);
            }
        }
        // Three modes per query:
        //   (a) caseInsensitive: LIKE on LOWER(fragment_text) with the trimmed phrase.
        //   (b) single token, case-sensitive: MATCH push-down to the GIN inverted index. The
        //       inline index uses `parser=english` which lowercases at index time, so the
        //       term is lowercased here so it round-trips through the analyzer.
        //   (c) multi-token, case-sensitive: per-token OR of LIKE substrings. Multiple MATCH
        //       predicates can't be OR'd in a single query — the engine raises
        //       "Match can only used as a pushdown predicate on column with GIN in a single
        //       query." — so we fall back to per-token LIKE which is semantically equivalent
        //       and runs without index push-down.
        if (request.caseInsensitive) {
            where.append("LOWER(f.fragment_text) LIKE '%").append(likeBody(trimmed.toLowerCase())).append("%'");
        } else if (validTokens.size() == 1) {
            where.append("f.fragment_text MATCH '")
                    .append(ContextSqlEscape.body(validTokens.get(0).toLowerCase())).append('\'');
        } else if (validTokens.size() > 1) {
            where.append('(');
            for (int i = 0; i < validTokens.size(); i++) {
                if (i > 0) {
                    where.append(" OR ");
                }
                where.append("LOWER(f.fragment_text) LIKE '%")
                        .append(likeBody(validTokens.get(i).toLowerCase())).append("%'");
            }
            where.append(')');
        } else {
            // No valid tokens (only punctuation) — fall back to LIKE on the raw phrase.
            where.append("f.fragment_text LIKE '%").append(likeBody(trimmed)).append("%'");
        }

        // Pair the fragment scan with a heads filter to drop stale versions and respect scope bounds.
        // The heads join is essential: a fragment row can outlive the current head (soft-delete,
        // replace) and we must not surface those. With a snapshot fence we substitute an as-of
        // view: for each entity find the max version whose snapshot_version <= fence, join the
        // version row, and use that as the "head" for the search — that's the unified-snapshot
        // contract from architecture doc §10.1.
        // BE rejects MATCH when it shares a WHERE with predicates from a JOINed table
        // ("Match can only used as a pushdown predicate on column with GIN in a single query.").
        // Isolate the MATCH/LIKE to an inner single-table scan on fragments, then JOIN heads on
        // the outer level so the heads filters land on a different scan node.
        StringBuilder sql = new StringBuilder();
        if (request.snapshotFence < 0) {
            sql.append("SELECT h.entity_id, f.version, f.fragment_id, f.fragment_kind, f.line_start, "
                            + "f.line_end, f.fragment_text, h.current_confidence AS confidence, "
                            + "f.token_count ")
                    .append("FROM (")
                    .append("SELECT entity_id, version, fragment_id, fragment_kind, line_start, "
                            + "line_end, fragment_text, token_count FROM ")
                    .append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.FRAGMENTS)
                    .append(" WHERE ").append(where.toString().replace("f.fragment_text", "fragment_text")
                            .replace("LOWER(f.fragment_text)", "LOWER(fragment_text)"));
            // Push the contextbase/collection scope INTO the inner fragment scan, not only the outer
            // heads JOIN. The LIMIT below caps the *pattern* match; without scope here it caps the
            // whole-table match across every contextbase, so the requested scope's fragments can be
            // dropped (or nondeterministically kept) before they are scoped — missing / flaky hits
            // once the corpus holds > maxFragmentScan fragments matching the pattern table-wide. The
            // ORDER BY makes the budget truncation a stable prefix of the scoped set.
            appendFragmentScopeFilter(sql, request);
            sql.append(" ORDER BY entity_id, version, fragment_id")
                    .append(" LIMIT ").append(request.maxFragmentScan)
                    .append(") f ")
                    .append("JOIN ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.HEADS).append(" h ")
                    .append("ON h.entity_id = f.entity_id AND h.current_version = f.version ")
                    .append("WHERE h.current_deleted = false ");
            if (request.contextBaseId != null) {
                sql.append("AND h.contextbase_id = ").append(request.contextBaseId).append(' ');
            } else if (request.contextBaseIds != null && !request.contextBaseIds.isEmpty()) {
                sql.append("AND h.contextbase_id IN (").append(joinIds(request.contextBaseIds)).append(") ");
            }
            if (request.collectionId != null) {
                sql.append("AND h.collection_id = ").append(request.collectionId).append(' ');
            } else if (request.collectionIds != null && !request.collectionIds.isEmpty()) {
                sql.append("AND h.collection_id IN (").append(joinIds(request.collectionIds)).append(") ");
            }
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND h.entity_type = ").append(ContextSqlEscape.literal(request.entityType)).append(' ');
            }
            if (request.confidenceMin != null) {
                sql.append("AND h.current_confidence >= ").append(request.confidenceMin).append(' ');
            }
        } else {
            // As-of path: derive each entity's visible version, join the version row, then join
            // fragments. The version row's `confidence` and `entity_type` carry the as-of state.
            // BE rejects MATCH that shares a WHERE with predicates from a JOINed table.
            // Push MATCH into an inner single-table scan on fragments, then JOIN heads / versions
            // outside so the as-of and deleted/entity_type filters land on a separate scan node.
            String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
            String fragmentPredicate = where.toString().replace("f.fragment_text", "fragment_text")
                    .replace("LOWER(f.fragment_text)", "LOWER(fragment_text)");
            sql.append("SELECT v.entity_id, v.version, f.fragment_id, f.fragment_kind, f.line_start, "
                            + "f.line_end, f.fragment_text, v.confidence, f.token_count ")
                    .append("FROM (SELECT entity_id, version, fragment_id, fragment_kind, ")
                    .append("line_start, line_end, fragment_text, token_count FROM ")
                    .append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.FRAGMENTS)
                    .append(" WHERE ").append(fragmentPredicate);
            // Same scope-pushdown as the non-snapshot path: bound the maxFragmentScan budget to the
            // requested scope so a whole-table pattern match cannot crowd this scope's fragments out
            // of the LIMIT before scoping. ORDER BY makes the truncation a stable prefix.
            appendFragmentScopeFilter(sql, request);
            sql.append(" ORDER BY entity_id, version, fragment_id")
                    .append(" LIMIT ").append(Math.max(1, request.maxFragmentScan))
                    .append(") f ")
                    .append("JOIN (")
                    .append("SELECT entity_id, MAX(version) AS av FROM ").append(versions)
                    .append(" WHERE snapshot_version <= ").append(request.snapshotFence);
            if (request.contextBaseId != null) {
                sql.append(" AND contextbase_id = ").append(request.contextBaseId);
            } else if (request.contextBaseIds != null && !request.contextBaseIds.isEmpty()) {
                sql.append(" AND contextbase_id IN (").append(joinIds(request.contextBaseIds)).append(")");
            }
            if (request.collectionId != null) {
                sql.append(" AND collection_id = ").append(request.collectionId);
            } else if (request.collectionIds != null && !request.collectionIds.isEmpty()) {
                sql.append(" AND collection_id IN (").append(joinIds(request.collectionIds)).append(")");
            }
            sql.append(" GROUP BY entity_id) av ON av.entity_id = f.entity_id AND av.av = f.version ")
                    .append("JOIN ").append(versions).append(" v ON v.entity_id = f.entity_id "
                            + "AND v.version = f.version ")
                    .append("WHERE v.deleted = false ");
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND v.entity_type = ").append(ContextSqlEscape.literal(request.entityType)).append(' ');
            }
            if (request.confidenceMin != null) {
                sql.append("AND v.confidence >= ").append(request.confidenceMin).append(' ');
            }
        }

        // Wrap the per-fragment scan above with the same window-function entity fold the TVF path
        // uses (`ContextTvfRelationResolver.buildTextSearchSql`): rank fragments per entity by
        // section-preference + fragment_id, keep the top fragment per entity, derive `hit_count`
        // and `text_score` server-side, and apply offset/maxResults pagination on the entity-level
        // result. Turns the SQL result set directly into one row per entity, already sorted and paged.
        // text_score = BM25-lite: TF saturation + length normalization over the matched fragments'
        // token_count. tf = per-entity match count; dl = summed token_count of the entity's matched
        // fragments. No IDF (per-term document frequency isn't available in the fold). See Bm25.
        String tfExpr = "COUNT(*) OVER (PARTITION BY entity_id)";
        String dlExpr = "SUM(token_count) OVER (PARTITION BY entity_id)";
        String textScoreExpr = Bm25.scoreSql(tfExpr, dlExpr);
        StringBuilder folded = new StringBuilder();
        folded.append("SELECT entity_id, version, fragment_id, fragment_kind, line_start, line_end, ")
                .append("fragment_text, confidence, hit_count, text_score FROM (")
                .append("SELECT entity_id, version, fragment_id, fragment_kind, line_start, line_end, ")
                .append("fragment_text, confidence, ")
                .append(tfExpr).append(" AS hit_count, ")
                .append(textScoreExpr).append(" AS text_score, ")
                .append("ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ")
                .append("CASE WHEN LOWER(fragment_kind) = 'section' THEN 0 ELSE 1 END, fragment_id) ")
                .append("AS entity_rank FROM (")
                .append(sql)
                .append(") fragment_hits) entity_hits WHERE entity_rank = 1 ")
                .append("ORDER BY text_score DESC, entity_id ");
        int offset = Math.max(0, request.offset);
        int max = Math.max(1, request.maxResults);
        folded.append("LIMIT ").append(offset).append(", ").append(max);

        JsonArray rawRows = runQuery(folded.toString());

        // Each row is already entity-level: one row per entity, sorted by text_score DESC, paged.
        // Snippet expansion is still done on the FE (P1's grep-style `<line>:<text>` formatter is
        // the contract for the REST API — different from the TVF surface, which inlines
        // `array_slice(split(...))` and emits raw lines without numbering).
        List<EntityRow> entityRows = new ArrayList<>(rawRows.size());
        for (JsonElement row : rawRows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            entityRows.add(new EntityRow(
                    data.get(0).getAsLong(),
                    data.get(1).getAsLong(),
                    data.get(2).getAsLong(),
                    data.get(3).getAsString(),
                    data.get(4).isJsonNull() ? -1 : data.get(4).getAsInt(),
                    data.get(5).isJsonNull() ? -1 : data.get(5).getAsInt(),
                    data.get(6).isJsonNull() ? "" : data.get(6).getAsString(),
                    data.get(8).isJsonNull() ? 0 : data.get(8).getAsInt(),
                    data.get(9).isJsonNull() ? 0.0 : data.get(9).getAsDouble()));
        }

        int before = request.contextLines != null ? request.contextLines
                : (request.beforeLines == null ? 0 : request.beforeLines);
        int after = request.contextLines != null ? request.contextLines
                : (request.afterLines == null ? 0 : request.afterLines);

        // Bulk-fetch canonical markdown only for entities that actually need snippet expansion —
        // one round trip total, regardless of how many entities are in the page. -l / -c skip
        // snippets entirely so we don't even fetch in those cases.
        Map<EntityVersionKey, String> canonicalCache = Collections.emptyMap();
        boolean wantsExpansion = (before > 0 || after > 0)
                && !request.filenamesOnly && !request.countOnly;
        if (wantsExpansion) {
            Set<EntityVersionKey> toFetch = new LinkedHashSet<>();
            for (EntityRow r : entityRows) {
                if (r.lineStart > 0 && r.lineEnd > 0) {
                    toFetch.add(new EntityVersionKey(r.entityId, r.version));
                }
            }
            if (!toFetch.isEmpty()) {
                canonicalCache = bulkFetchCanonicalMarkdown(toFetch);
            }
        }

        List<EntityHit> hits = new ArrayList<>(entityRows.size());
        for (EntityRow r : entityRows) {
            FragmentHit top;
            if (request.filenamesOnly || request.countOnly) {
                top = null;
            } else {
                String snippet = r.fragmentPreview;
                if ((before > 0 || after > 0) && r.lineStart > 0 && r.lineEnd > 0) {
                    String canonical = canonicalCache.get(new EntityVersionKey(r.entityId, r.version));
                    if (canonical != null) {
                        snippet = sliceSnippetLines(canonical, r.lineStart, r.lineEnd, before, after,
                                r.fragmentPreview);
                    }
                }
                top = new FragmentHit(r.entityId, r.version, r.fragmentId, r.fragmentKind,
                        r.lineStart, r.lineEnd, snippet);
            }
            hits.add(new EntityHit(r.entityId, r.hitCount, top, r.textScore));
        }
        return hits;
    }

    /** Append {@code AND contextbase_id .../collection_id ...} scope on the fragments table. */
    private void appendFragmentScopeFilter(StringBuilder sql, Request request) {
        if (request.contextBaseId != null) {
            sql.append(" AND contextbase_id = ").append(request.contextBaseId);
        } else if (request.contextBaseIds != null && !request.contextBaseIds.isEmpty()) {
            sql.append(" AND contextbase_id IN (").append(joinIds(request.contextBaseIds)).append(')');
        }
        if (request.collectionId != null) {
            sql.append(" AND collection_id = ").append(request.collectionId);
        } else if (request.collectionIds != null && !request.collectionIds.isEmpty()) {
            sql.append(" AND collection_id IN (").append(joinIds(request.collectionIds)).append(')');
        }
    }

    /**
     * Fetch the source markdown columns for every {@code (entity_id, version)} pair in one round
     * trip and return a map keyed by that pair holding the canonicalized markdown text. Pairs that
     * do not resolve (entity dropped, version compacted, etc.) are simply absent from the returned
     * map; the caller falls back to the fragment preview for those.
     */
    private Map<EntityVersionKey, String> bulkFetchCanonicalMarkdown(Set<EntityVersionKey> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }
        StringBuilder sb = new StringBuilder(64 + keys.size() * 32);
        sb.append("SELECT entity_id, version, raw_markdown, body, frontmatter_json, source_json FROM ")
                .append(ContextInternalTables.DATABASE).append('.').append(ContextInternalTables.VERSIONS)
                .append(" WHERE ");
        // PK-friendly OR list: each clause is a full (entity_id, version) point lookup. Row-
        // constructor IN (`(entity_id, version) IN ((..,..),(..,..))`) is not portable across
        // StarRocks query path versions, so we build an OR chain explicitly.
        boolean first = true;
        for (EntityVersionKey k : keys) {
            if (!first) {
                sb.append(" OR ");
            }
            sb.append("(entity_id = ").append(k.entityId).append(" AND version = ").append(k.version).append(')');
            first = false;
        }
        JsonArray rows = runQuery(sb.toString());
        Map<EntityVersionKey, String> out = new HashMap<>(rows.size() * 2);
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            long entityId = data.get(0).getAsLong();
            long version = data.get(1).getAsLong();
            String rawMarkdown = data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsString() : null;
            String body = data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsString() : null;
            // frontmatter_json / source_json are JSON-typed columns: getAsString() on a JsonObject
            // throws UnsupportedOperationException("JsonObject"). Use toString() for non-primitive
            // JsonElements to round-trip the JSON text.
            String frontmatterJson = data.size() > 4 && !data.get(4).isJsonNull()
                    ? jsonRawString(data.get(4)) : null;
            String sourceJson = data.size() > 5 && !data.get(5).isJsonNull()
                    ? jsonRawString(data.get(5)) : null;
            out.put(new EntityVersionKey(entityId, version),
                    MarkdownExtractor.canonicalizeRawMarkdown(rawMarkdown, body, frontmatterJson, sourceJson));
        }
        return out;
    }

    /**
     * Pure string slice: take a canonical markdown body, render lines {@code [lineStart-before,
     * lineEnd+after]} as the {@code "<line-number>:<text>"} grep-style snippet that the caller
     * sees. Visible to tests.
     */
    static String sliceSnippetLines(String canonicalMarkdown, int lineStart, int lineEnd,
                                    int before, int after, String fallbackSnippet) {
        if (canonicalMarkdown == null) {
            return fallbackSnippet;
        }
        String[] lines = canonicalMarkdown.split("\n", -1);
        int from = Math.max(1, lineStart - before);
        int to = Math.min(lines.length, lineEnd + after);
        StringBuilder snippet = new StringBuilder();
        for (int i = from; i <= to; i++) {
            if (snippet.length() > 0) {
                snippet.append('\n');
            }
            snippet.append(i).append(':').append(lines[i - 1]);
        }
        return snippet.length() == 0 ? fallbackSnippet : snippet.toString();
    }

    private String joinIds(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(ids.get(i));
        }
        return sb.toString();
    }

    /**
     * Escape a user string for embedding inside a {@code LIKE '%...%'} single-quoted literal. First
     * escape the LIKE special characters ({@code \ % _}) so they match literally instead of acting
     * as wildcards, then run the shared {@link ContextSqlEscape#body} so a backslash or quote cannot
     * break out of the SQL string literal (StarRocks treats backslash as a string-literal escape, so
     * doubling only the quote is not injection-safe — see the escaper's contract).
     */
    private static String likeBody(String s) {
        if (s == null) {
            return "";
        }
        String likeEscaped = s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
        return ContextSqlEscape.body(likeEscaped);
    }

    private static String jsonRawString(JsonElement el) {
        if (el == null || el.isJsonNull()) {
            return null;
        }
        return el.isJsonPrimitive() ? el.getAsString() : el.toString();
    }

    // Visible to tests so they can stub the SQL plane without spinning up a real StarRocks.
    JsonArray runQuery(String sql) {
        return ContextSqlSupport.executeDql(sql);
    }

    /**
     * Carrier for one entity-level row coming back from the SQL fold. The fold (window functions
     * + {@code WHERE entity_rank = 1}) emits one of these per entity already paginated and
     * sorted by {@code text_score DESC}; the FE then bolts on the snippet (preview by default,
     * or grep-style expansion when the request asked for context lines).
     */
    private static final class EntityRow {
        final long entityId;
        final long version;
        final long fragmentId;
        final String fragmentKind;
        final int lineStart;
        final int lineEnd;
        final String fragmentPreview;
        final int hitCount;
        final double textScore;

        EntityRow(long entityId, long version, long fragmentId, String fragmentKind,
                int lineStart, int lineEnd, String fragmentPreview, int hitCount, double textScore) {
            this.entityId = entityId;
            this.version = version;
            this.fragmentId = fragmentId;
            this.fragmentKind = fragmentKind;
            this.lineStart = lineStart;
            this.lineEnd = lineEnd;
            this.fragmentPreview = fragmentPreview;
            this.hitCount = hitCount;
            this.textScore = textScore;
        }
    }

    /** Map key for the bulk markdown cache. Equality and hashCode cover both fields. */
    static final class EntityVersionKey {
        final long entityId;
        final long version;

        EntityVersionKey(long entityId, long version) {
            this.entityId = entityId;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EntityVersionKey)) {
                return false;
            }
            EntityVersionKey other = (EntityVersionKey) o;
            return entityId == other.entityId && version == other.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityId, version);
        }
    }
}
