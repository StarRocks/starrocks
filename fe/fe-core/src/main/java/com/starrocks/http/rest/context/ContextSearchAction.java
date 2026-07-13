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

package com.starrocks.http.rest.context;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.retrieval.ContextScopeResolver;
import com.starrocks.context.retrieval.ContextSearchExecutor;
import com.starrocks.context.retrieval.ReferenceExpander;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/search}. Body:
 * <pre>{@code
 * {
 *   "contextbase": "sales_ai",
 *   "collection": "pipeline_rules",
 *   "query_text": "deal scoring",
 *   "graph_mode": "AUTO",
 *   "max_results": 10,
 *   "text_weight": 0.1, "vector_weight": 0.6, "graph_weight": 0.3
 * }
 * }</pre>
 *
 * <p>The three weights are optional. Any weight the caller omits keeps the executor's
 * vector-dominant default ({@code ContextSearchExecutor.DEFAULT_*_WEIGHT}); the values shown above
 * are those defaults, not a required payload. Only supplied weights override, and only a supplied
 * weight is treated as explicit (which suppresses retrieval-profile overrides for that weight).
 *
 * <p>{@code graph_mode} is {@code AUTO} (default) or {@code OFF}. With AUTO, graph seeds are
 * auto-derived from text/vector top-K, so callers normally do not pass {@code seed_ids}. The
 * optional {@code graph_seed_topk} (int, default {@code min(max_results, 10)}) tunes how many
 * candidates feed the reference expansion. {@code seed_ids} remains as a power-user override
 * that composes (union, dedup) with the auto-derived seeds.
 */
public class ContextSearchAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(ContextSearchAction.class);
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextSearchAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/search", new ContextSearchAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, com.starrocks.authorization.AccessDeniedException {
        try {
            String body = request.getContent();
            if (Strings.isNullOrEmpty(body)) {
                sendResult(request, response, new RestBaseResult("body is required"));
                return;
            }
            Map<String, Object> payload = GSON.fromJson(body, MAP_TYPE);

            ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
            String scope = (String) payload.get("scope");
            req.contextBase = (String) payload.get("contextbase");
            @SuppressWarnings("unchecked")
            List<String> contextbases = (List<String>) payload.get("contextbases");
            req.collection = (String) payload.get("collection");
            @SuppressWarnings("unchecked")
            List<String> collections = (List<String>) payload.get("collections");
            String collectionType = (String) payload.get("collection_type");
            req.queryText = (String) payload.get("query_text");
            req.queryEmbedding = parseEmbedding(payload.get("query_embedding"));
            req.allowStaleVector = !Boolean.FALSE.equals(payload.get("allow_stale_vector"));
            req.entityType = (String) payload.get("entity_type");
            req.workspace = (String) payload.get("workspace");
            req.retrievalProfile = (String) payload.get("retrieval_profile");
            @SuppressWarnings("unchecked")
            Map<String, Object> filters = (Map<String, Object>) payload.get("filters");
            req.filters = filters;
            req.consistency = (String) payload.get("consistency");

            boolean hasMultiBases = contextbases != null && !contextbases.isEmpty();
            if (Strings.isNullOrEmpty(req.contextBase) && Strings.isNullOrEmpty(scope) && !hasMultiBases) {
                sendResult(request, response,
                        new RestBaseResult("\"contextbase\", \"contextbases\" or \"scope\" is required"));
                return;
            }
            ContextScopeResolver.ResolvedScope resolvedScope;
            if (hasMultiBases) {
                if (!Strings.isNullOrEmpty(req.contextBase) || !Strings.isNullOrEmpty(scope)) {
                    sendResult(request, response, new RestBaseResult(
                            "use either \"contextbase\"/\"scope\" or \"contextbases\", not both"));
                    return;
                }
                resolvedScope = ContextScopeResolver.resolveContextBases(
                        GlobalStateMgr.getCurrentState().getContextMgr(), contextbases,
                        req.collection, collections, collectionType);
            } else {
                resolvedScope = ContextScopeResolver.resolve(
                        GlobalStateMgr.getCurrentState().getContextMgr(), scope, req.contextBase,
                        req.collection, collections, collectionType);
            }
            req.contextBase = resolvedScope.contextBase;
            req.collection = resolvedScope.collection;
            req.collectionIdOverride = resolvedScope.collectionId;
            req.collectionIdsOverride = resolvedScope.collectionIds;
            if (resolvedScope.isMultiContextBase()) {
                req.contextBaseIdsOverride = resolvedScope.contextBaseIds;
            } else {
                req.contextBaseIdOverride = resolvedScope.contextBaseId;
            }
            // Per-base authorization: every contextbase in scope must pass USAGE. Any failure
            // rejects the whole request (no silent filtering — that would leak base existence).
            for (String base : resolvedScope.contextBases) {
                ContextRestAuth.checkOnContextBase(ConnectContext.get(), base,
                        ContextRestAuth.BaseAction.USAGE);
            }

            @SuppressWarnings("unchecked")
            List<Number> seedList = (List<Number>) payload.get("seed_ids");
            if (seedList != null) {
                List<Long> seeds = new ArrayList<>(seedList.size());
                for (Number n : seedList) {
                    seeds.add(n.longValue());
                }
                req.seedIds = seeds;
            }

            Number maxResults = (Number) payload.getOrDefault("max_results", 20L);
            req.maxResults = maxResults.intValue();
            Number maxTokens = (Number) payload.getOrDefault("max_tokens", 4000L);
            req.maxTokens = maxTokens.intValue();
            String graphMode = ((String) payload.getOrDefault("graph_mode", "AUTO"))
                    .toUpperCase(java.util.Locale.ROOT);
            if ("REQUIRED".equals(graphMode)) {
                // REQUIRED was removed when fusion gained auto-seed-derivation. Surface a loud
                // signal so existing programmatic callers update rather than silently get AUTO.
                throw new com.starrocks.context.error.ContextException(
                        com.starrocks.context.error.ContextErrorCode.INVALID_ARGUMENT,
                        "graph_mode=REQUIRED is no longer supported; use AUTO (default) or OFF. "
                                + "For strict graph traversal, call /api/context/graph-expand "
                                + "with require_complete=true.");
            }
            try {
                req.graphMode = ContextSearchExecutor.GraphMode.valueOf(graphMode);
            } catch (IllegalArgumentException e) {
                throw new com.starrocks.context.error.ContextException(
                        com.starrocks.context.error.ContextErrorCode.INVALID_ARGUMENT,
                        "invalid graph_mode '" + payload.get("graph_mode") + "'; expected AUTO or OFF");
            }
            applyWeights(payload, req);
            req.graphDepth = ((Number) payload.getOrDefault("graph_depth", 2L)).intValue();
            req.maxFrontier = ((Number) payload.getOrDefault("max_frontier", 200L)).intValue();
            req.graphSeedTopK = ((Number) payload.getOrDefault("graph_seed_topk", 0L)).intValue();
            req.graphStrategy = (String) payload.get("graph_strategy");
            req.explicitGraphStrategy = payload.containsKey("graph_strategy");
            String direction = (String) payload.get("direction");
            if (direction != null) {
                try {
                    req.direction = ReferenceExpander.Direction.valueOf(
                            direction.trim().toUpperCase(java.util.Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new com.starrocks.context.error.ContextException(
                            com.starrocks.context.error.ContextErrorCode.INVALID_ARGUMENT,
                            "invalid direction '" + direction + "'; expected FORWARD, BACKWARD or BOTH");
                }
            } else {
                req.direction = ContextSearchExecutor.defaultGraphDirection();
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> stratOpts = (Map<String, Object>) payload.get("strategy_options");
            req.strategyOptions = stratOpts;

            @SuppressWarnings("unchecked")
            List<String> edgeTypes = (List<String>) payload.get("edge_types");
            req.edgeTypes = edgeTypes;
            req.asOfTime = (String) payload.get("as_of_time");
            Number snapshotVersion = (Number) payload.get("snapshot_version");
            if (snapshotVersion != null) {
                req.snapshotVersion = snapshotVersion.longValue();
            }

            // Optional cross-encoder second-phase rerank (default OFF).
            req.rerank = Boolean.TRUE.equals(payload.get("rerank"));
            req.explicitRerank = payload.containsKey("rerank");
            req.rerankProvider = (String) payload.get("rerank_provider");
            Number rerankTopN = (Number) payload.get("rerank_top_n");
            if (rerankTopN != null) {
                req.rerankTopN = rerankTopN.intValue();
            }
            req.rerankUseBody = Boolean.TRUE.equals(payload.get("rerank_use_body"));
            req.explicitRerankUseBody = payload.containsKey("rerank_use_body");

            ContextSearchExecutor.Result result = GlobalStateMgr.getCurrentState()
                    .getContextSearchExecutor().search(req);

            SearchResponse resp = new SearchResponse();
            resp.requestId = ContextRestAuth.currentRequestId();
            resp.candidates = new ArrayList<>();
            List<Long> ids = new ArrayList<>(result.candidates.size());
            for (ContextSearchExecutor.Candidate c : result.candidates) {
                ids.add(c.entityId);
            }
            Map<Long, ContextReadExecutor.EntityMeta> metaById = GlobalStateMgr.getCurrentState()
                    .getContextReadExecutor().loadEntityMetadata(ids,
                            result.explain.get("snapshot_fence") instanceof Number
                                    ? ((Number) result.explain.get("snapshot_fence")).longValue() : -1L);
            for (ContextSearchExecutor.Candidate c : result.candidates) {
                ContextReadExecutor.EntityMeta meta = metaById.get(c.entityId);
                CandidateEntry e = new CandidateEntry();
                e.id = c.entityId;
                if (meta != null) {
                    e.entityKey = meta.entityKey;
                    e.entityType = meta.entityType;
                    e.preview = meta.preview;
                    e.version = meta.version;
                    e.snapshotVersion = meta.snapshotVersion;
                    e.title = meta.title;
                    e.frontmatterJson = meta.frontmatterJson;
                }
                e.textScore = c.textScore;
                e.vectorScore = c.vectorScore;
                e.graphScore = c.graphScore;
                e.finalScore = c.finalScore;
                e.hopCount = c.hopCount;
                e.edgeTypes = c.edgeTypes;
                e.snippet = c.snippet;
                resp.candidates.add(e);
            }
            resp.explain = result.explain;
            resp.packedText = (String) result.explain.get("packed_text");
            resp.usedTokensEstimate = numberValue(result.explain.get("used_tokens_estimate"));
            resp.includedEntities = longList(result.explain.get("included_entities"));
            resp.truncatedEntities = longList(result.explain.get("truncated_entities"));
            @SuppressWarnings("unchecked")
            Map<String, Object> disclosureLevels = (Map<String, Object>) result.explain.get("disclosure_levels");
            resp.disclosureLevels = disclosureLevels;
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        } catch (Exception e) {
            // Catch-all so unexpected exceptions (NPE on null result.explain, ClassCastException
            // on Map<String,Object> vs JsonObject, etc.) don't bubble to Netty leaving the
            // response with HTTP 200 + empty body. Log the stack server-side so the failure is
            // debuggable, but return a generic message to the client. Previously the exception
            // class name and message were echoed back verbatim — Status messages routinely
            // contain internal table names, fragments of generated SQL, and stack-trace context,
            // any of which leak implementation details to unauthenticated callers and shorten
            // the recon phase of an attack.
            LOG.warn("/api/context/search failed", e);
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(
                            new com.starrocks.context.error.ContextException(
                                    com.starrocks.context.error.ContextErrorCode.INTERNAL_ERROR,
                                    "internal error processing context search request"),
                            ContextRestAuth.currentRequestId()));
        }
    }

    /**
     * Parse the three fusion weights from the request body into {@code req}. Extracted so the
     * weight-resolution contract can be unit-tested without the full HTTP/auth/scope machinery.
     *
     * <p>A weight is overridden only when the caller actually supplies the key. When a key is
     * absent the field is left at its {@link ContextSearchExecutor.Request} construction-time
     * default (the vector-dominant {@code DEFAULT_*_WEIGHT}) and stays non-explicit, so a retrieval
     * profile can still set it downstream (see {@code ContextSearchExecutor#applyProfile}). A
     * literal fallback here would shadow the executor default and silently pin every weightless
     * REST call to that literal regardless of the configured default.
     */
    static void applyWeights(Map<String, Object> payload, ContextSearchExecutor.Request req) {
        if (payload.containsKey("text_weight")) {
            req.textWeight = ((Number) payload.get("text_weight")).doubleValue();
            req.explicitTextWeight = true;
        }
        if (payload.containsKey("vector_weight")) {
            req.vectorWeight = ((Number) payload.get("vector_weight")).doubleValue();
            req.explicitVectorWeight = true;
        }
        if (payload.containsKey("graph_weight")) {
            req.graphWeight = ((Number) payload.get("graph_weight")).doubleValue();
            req.explicitGraphWeight = true;
        }
    }

    private static final class SearchResponse {
        @JsonProperty("request_id")
        public String requestId;
        public List<CandidateEntry> candidates;
        public Map<String, Object> explain;

        @JsonProperty("packed_text")
        public String packedText;

        @JsonProperty("used_tokens_estimate")
        public Long usedTokensEstimate;

        @JsonProperty("included_entities")
        public List<Long> includedEntities;

        @JsonProperty("truncated_entities")
        public List<Long> truncatedEntities;

        @JsonProperty("disclosure_levels")
        public Map<String, Object> disclosureLevels;
    }

    private static final class CandidateEntry {
        public long id;

        @JsonProperty("entity_key")
        public String entityKey;

        @JsonProperty("entity_type")
        public String entityType;

        public String title;
        public String preview;
        public long version;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;

        @JsonProperty("text_score")
        public double textScore;

        @JsonProperty("vector_score")
        public double vectorScore;

        @JsonProperty("graph_score")
        public double graphScore;

        @JsonProperty("final_score")
        public double finalScore;

        @JsonProperty("hop_count")
        public int hopCount;

        @JsonProperty("edge_types")
        public List<String> edgeTypes;

        public String snippet;

        // Verbatim frontmatter JSON for the entity's current version. Lets clients recover the
        // ingest-time `path` (and any other frontmatter keys) without a follow-up
        // /api/context/get round-trip per hit.
        @JsonProperty("frontmatter_json")
        public String frontmatterJson;
    }

    private float[] parseEmbedding(Object raw) {
        if (!(raw instanceof List<?> list) || list.isEmpty()) {
            return null;
        }
        float[] out = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object value = list.get(i);
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException("query_embedding must contain numeric values");
            }
            out[i] = ((Number) value).floatValue();
        }
        return out;
    }

    private Long numberValue(Object raw) {
        return raw instanceof Number ? ((Number) raw).longValue() : null;
    }

    private List<Long> longList(Object raw) {
        if (!(raw instanceof List<?> list)) {
            return null;
        }
        List<Long> values = new ArrayList<>();
        for (Object value : list) {
            if (value instanceof Number) {
                values.add(((Number) value).longValue());
            }
        }
        return values;
    }
}
