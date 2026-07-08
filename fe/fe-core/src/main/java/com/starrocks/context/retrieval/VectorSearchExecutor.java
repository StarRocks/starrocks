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
import com.starrocks.context.ContextSqlSupport;
import com.starrocks.context.embedding.FeEmbeddingClient;
import com.starrocks.context.error.ContextException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Standalone vector retrieval over {@link ContextInternalTables#FRAGMENTS}.
 *
 * <p>The heavy math runs in the storage layer via
 * {@code approx_cosine_similarity(query_vec, f.embedding) ORDER BY score DESC LIMIT k}, which lets
 * {@code RewriteToVectorPlanRule} route the scan through the HNSW index when one is present.
 * The FE resolves the query embedding and folds fragment-level hits back to entity-level results.
 */
public class VectorSearchExecutor {

    private static final Logger LOG = LogManager.getLogger(VectorSearchExecutor.class);

    public static final class Request {
        public String queryText;
        public float[] queryEmbedding;
        public boolean allowStaleVector = true;
        public boolean deepMode;
        // Explicit fragment selector: "preview" | "section" | "both" (null = default).
        // Overrides deepMode when set. Lets callers pin entity-level (preview) vs passage-level
        // (section) vs combined recall — e.g. A/B benchmarking the preview-only vs preview+section
        // retrieval quality. Default (null) searches both.
        public String fragmentMode;
        public boolean idsOnly;
        public boolean includeFrontmatter;
        public Long contextBaseId;
        // Multi-contextbase scope. Used only when contextBaseId is null (single value wins). Mirrors
        // the collectionId / collectionIds pattern: filters f.contextbase_id IN (...).
        public List<Long> contextBaseIds;
        public Long collectionId;
        public List<Long> collectionIds;
        public String entityType;
        public Double confidenceMin;
        public int maxFragmentScan = 2000;
        public int maxResults = 50;
        public int offset = 0;
        public long snapshotFence = -1L;
        // Filled by search(): wall time spent resolving the query embedding (provider round trip) vs.
        // the ANN scan + entity fold. Read by ContextSearchExecutor for its per-step timing log. Safe
        // to read after the vector future joins (the join establishes happens-before on this object).
        public long embedNanos;
        public long annNanos;
    }

    /**
     * Entity-level vector hit. {@code score} is in [0, 1]: cosine similarity rescaled from
     * [-1, 1] via {@code (cos + 1) / 2}. The winning fragment metadata is kept so standalone
     * vector search can surface the preview/section that actually matched.
     */
    public static final class EntityHit {
        public final long entityId;
        public final double score;
        public final String fragmentKind;
        public final String snippet;

        public EntityHit(long entityId, double score, String fragmentKind, String snippet) {
            this.entityId = entityId;
            this.score = score;
            this.fragmentKind = fragmentKind;
            this.snippet = snippet;
        }
    }

    public List<EntityHit> search(Request request) {
        long tEmbed = System.nanoTime();
        float[] queryEmbedding = resolveQueryEmbedding(request);
        request.embedNanos = System.nanoTime() - tEmbed;
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            return new ArrayList<>();
        }
        long tAnn = System.nanoTime();
        List<EntityHit> hits = runFoldedQuery(buildSearchSql(vectorLiteral(queryEmbedding), request), request);
        request.annNanos = System.nanoTime() - tAnn;
        return hits;
    }

    private static String vectorLiteral(float[] queryEmbedding) {
        StringBuilder vec = new StringBuilder("[");
        for (int i = 0; i < queryEmbedding.length; ++i) {
            if (i > 0) {
                vec.append(',');
            }
            vec.append(Float.toString(queryEmbedding[i]));
        }
        vec.append(']');
        return vec.toString();
    }

    /**
     * Build the two-level vector-search SQL: an inner TopN ANN scan wrapped by an outer
     * current-version / as-of visibility JOIN. Package-private so shape tests can lock the SQL
     * (in particular the as-of snapshot fence on the inner scan) without a live cluster.
     */
    static String buildSearchSql(String vectorLiteral, Request request) {
        int scanLimit = Math.max(request.maxFragmentScan,
                Math.max(1, request.offset) + Math.max(1, request.maxResults) * 4);
        // Default (incl. the fusion path, which never sets deepMode) searches BOTH preview and
        // section fragments; the per-entity ROW_NUMBER fold below keeps the best-scoring fragment
        // per entity, so a long doc is reachable via its section fragments rather than only its
        // truncated preview. deepMode (-d) stays section-only for passage-focused callers; an
        // explicit fragmentMode overrides both.
        String fragmentFilter = resolveFragmentFilter(request);

        // ---- inner ANN: TopN directly on the fragments scan, with the SCOPE as scan residual ----
        // contextbase_id / collection_id / fragment_kind are columns of context_entity_fragments
        // (contextbase_id / collection_id are denormalized from heads; they are immutable per
        // entity). Putting them ON the scan -- not on a heads JOIN -- lets RewriteToVectorPlanRule
        // keep them as residual predicates and the BE pre-filter the candidate rows before the ANN
        // search (scoped HNSW). The ORDER BY ... LIMIT sits directly on the scan (no JOIN below the
        // limit), which is required for the index rewrite AND shields the scan from the outer
        // version/deleted JOIN (a filter above a LIMIT is post-limit and does not disable the index).
        StringBuilder ann = new StringBuilder();
        ann.append("SELECT f.entity_id AS entity_id, f.version AS version, ")
                .append("f.fragment_kind AS fragment_kind, f.fragment_text AS fragment_text, ")
                .append("approx_cosine_similarity(").append(vectorLiteral).append(", f.embedding) AS score ")
                .append("FROM ").append(ContextInternalTables.DATABASE).append('.')
                .append(ContextInternalTables.FRAGMENTS).append(" f ")
                .append("WHERE 1 = 1 ");
        if (fragmentFilter != null) {
            ann.append("AND ").append(fragmentFilter).append(' ');
        }
        if (request.contextBaseId != null) {
            ann.append("AND f.contextbase_id = ").append(request.contextBaseId).append(' ');
        } else if (request.contextBaseIds != null && !request.contextBaseIds.isEmpty()) {
            ann.append("AND f.contextbase_id IN (").append(joinIds(request.contextBaseIds)).append(") ");
        }
        if (request.collectionId != null) {
            ann.append("AND f.collection_id = ").append(request.collectionId).append(' ');
        } else if (request.collectionIds != null && !request.collectionIds.isEmpty()) {
            ann.append("AND f.collection_id IN (").append(joinIds(request.collectionIds)).append(") ");
        }
        // As-of reads must fence the fragments BEFORE the TopN. f.snapshot_version is denormalized
        // onto the fragments table, so bounding it here keeps future-version fragments from filling
        // scanLimit and then being discarded by the outer as-of version JOIN (which would leave the
        // visible older versions missing / the page short or empty). It also lets the BE pre-filter
        // the scan. The outer versions subquery still resolves the exact as-of version per entity.
        if (request.snapshotFence >= 0) {
            ann.append("AND f.snapshot_version <= ").append(request.snapshotFence).append(' ');
        }
        ann.append("ORDER BY score DESC LIMIT ").append(scanLimit);

        // ---- outer: enforce current-version / not-deleted (and entity_type / confidence) ----
        // These live on heads (mutable: version bumps, deletes) so they cannot be denormalized;
        // they stay above the inner TopN and only drop a few scoped candidates.
        StringBuilder sql = new StringBuilder();
        if (request.snapshotFence < 0) {
            sql.append("SELECT ann.entity_id AS entity_id, ann.score AS score, ")
                    .append("ann.fragment_kind AS fragment_kind, ann.fragment_text AS fragment_text ")
                    .append("FROM (").append(ann).append(") ann ")
                    .append("JOIN ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.HEADS).append(" h ")
                    .append("ON h.entity_id = ann.entity_id AND h.current_version = ann.version ")
                    .append("WHERE h.current_deleted = false ");
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND h.entity_type = '")
                        .append(request.entityType.replace("'", "''")).append("' ");
            }
            if (request.confidenceMin != null) {
                sql.append("AND h.current_confidence >= ").append(request.confidenceMin).append(' ');
            }
        } else {
            String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
            sql.append("SELECT ann.entity_id AS entity_id, ann.score AS score, ")
                    .append("ann.fragment_kind AS fragment_kind, ann.fragment_text AS fragment_text ")
                    .append("FROM (").append(ann).append(") ann ")
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
            sql.append(" GROUP BY entity_id) av ON av.entity_id = ann.entity_id AND av.av = ann.version ")
                    .append("JOIN ").append(versions).append(" v ON v.entity_id = ann.entity_id "
                            + "AND v.version = ann.version ")
                    .append("WHERE v.deleted = false ");
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND v.entity_type = '")
                        .append(request.entityType.replace("'", "''")).append("' ");
            }
            if (request.confidenceMin != null) {
                sql.append("AND v.confidence >= ").append(request.confidenceMin).append(' ');
            }
        }
        return sql.toString();
    }

    /**
     * Wrap a per-fragment SELECT (columns: entity_id, score, fragment_kind, fragment_text) with
     * the same window-function entity fold the TVF path uses
     * (`ContextTvfRelationResolver.buildVectorSearchSql`): per-entity rank by descending raw
     * cosine, keep `entity_rank = 1`, normalize via `(score + 1.0) / 2.0`, then ORDER BY
     * vector_score DESC, entity_id and apply offset/maxResults pagination. Replaces the prior
     * Java best-score-per-entity LinkedHashMap loop and the post-fold sort/sublist with a single
     * SQL pass that returns one row per entity already paged.
     */
    private List<EntityHit> runFoldedQuery(String fragmentSql, Request request) {
        StringBuilder folded = new StringBuilder();
        folded.append("SELECT entity_id, (raw_score + 1.0) / 2.0 AS vector_score, ")
                .append("fragment_kind, fragment_text FROM (")
                .append("SELECT entity_id, score AS raw_score, fragment_kind, fragment_text, ")
                .append("ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY score DESC) AS entity_rank ")
                .append("FROM (").append(fragmentSql).append(") fragment_hits) entity_hits ")
                .append("WHERE entity_rank = 1 AND raw_score IS NOT NULL ")
                .append("ORDER BY vector_score DESC, entity_id ");
        int offset = Math.max(0, request.offset);
        int max = Math.max(1, request.maxResults);
        folded.append("LIMIT ").append(offset).append(", ").append(max);

        JsonArray rows = runQuery(folded.toString());
        List<EntityHit> hits = new ArrayList<>(rows.size());
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            long entityId = data.get(0).getAsLong();
            JsonElement scoreElem = data.get(1);
            if (scoreElem.isJsonNull()) {
                continue;
            }
            double score;
            try {
                score = scoreElem.getAsDouble();
            } catch (Exception e) {
                continue;
            }
            String hitKind = data.get(2).isJsonNull() ? null : data.get(2).getAsString();
            String snippet = data.get(3).isJsonNull() ? null : data.get(3).getAsString();
            hits.add(new EntityHit(entityId, score, hitKind, snippet));
        }
        return hits;
    }

    /**
     * Resolve the {@code f.fragment_kind} WHERE clause for a request, or null when no filter is
     * needed. An explicit {@code fragmentMode} ("preview"/"section"/"both") wins; otherwise
     * {@code deepMode} picks section-only and the default is both. "Both" returns null rather
     * than {@code IN ('preview', 'section')}: the writer only ever emits those two kinds, so the
     * IN is a tautology -- but as a scan residual it would force the BE vector pre-filter to read
     * and evaluate fragment_kind over the whole scan range on every query for nothing. Values
     * are hardcoded literals (never user data).
     */
    private static String resolveFragmentFilter(Request request) {
        String mode = request.fragmentMode == null ? "" : request.fragmentMode.trim().toLowerCase();
        if (mode.isEmpty()) {
            // No explicit selector: -d (deepMode) means section-only; default is both.
            return request.deepMode ? "f.fragment_kind = 'section'" : null;
        }
        // Explicit fragmentMode wins over deepMode.
        if ("preview".equals(mode)) {
            return "f.fragment_kind = 'preview'";
        }
        if ("section".equals(mode)) {
            return "f.fragment_kind = 'section'";
        }
        return null;  // "both" or unknown: every fragment kind, no filter needed
    }

    public float[] resolveQueryEmbedding(Request request) {
        if (request.queryEmbedding != null && request.queryEmbedding.length > 0) {
            return request.queryEmbedding;
        }
        if (Strings.isNullOrEmpty(request.queryText)) {
            return null;
        }
        // Resolve the query embedding on the FE via the configured DEFAULT EMBEDDING PROVIDER.
        // We call the provider directly (FeEmbeddingClient) rather than running a
        // `SELECT embedding(text, parse_json(cfg))` through SimpleExecutor: EmbeddingConfigJson
        // inlines the provider api_key verbatim into that config JSON, and SimpleExecutor audits
        // every DQL through SqlCredentialRedactor, which only redacts `key = value` assignments --
        // not JSON `"api_key":"..."`. Routing the config through DQL would therefore leak the raw
        // key into the FE internal audit/error logs. The FE client keeps the key on the HTTP path.
        try {
            List<float[]> vectors = FeEmbeddingClient.embedBatch(java.util.Collections.singletonList(request.queryText));
            if (vectors.isEmpty() || vectors.get(0) == null || vectors.get(0).length == 0) {
                return null;
            }
            return vectors.get(0);
        } catch (ContextException e) {
            // No DEFAULT EMBEDDING PROVIDER, or the provider call failed. In stale-tolerant mode we
            // degrade to "no vector hits"; when the caller demanded fresh vectors we must surface
            // the failure (VECTOR_NOT_READY) rather than silently returning an empty result.
            if (request.allowStaleVector) {
                LOG.debug("vector_search: FE embedding unavailable: {}", e.getMessage());
                return null;
            }
            throw e;
        }
    }

    private static String joinIds(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(ids.get(i));
        }
        return sb.toString();
    }

    // Visible to tests so they can stub the SQL plane without spinning up a real cluster.
    protected JsonArray runQuery(String sql) {
        try {
            return ContextSqlSupport.executeDql(sql);
        } catch (Exception e) {
            LOG.debug("vector_search query failed (table not ready?): {}", e.getMessage());
            return new JsonArray();
        }
    }
}
