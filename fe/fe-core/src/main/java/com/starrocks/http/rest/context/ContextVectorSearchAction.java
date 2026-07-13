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
import com.starrocks.context.retrieval.VectorSearchExecutor;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/vector-search}. Body:
 * {@code {"scope":"sales_ai.pipeline_rules","query_text":"deal scoring","options":"-d","limit":10}}.
 */
public class ContextVectorSearchAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextVectorSearchAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/vector-search",
                new ContextVectorSearchAction(controller));
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
            String queryText = (String) payload.get("query_text");
            float[] queryEmbedding = parseEmbedding(payload.get("query_embedding"));
            if (Strings.isNullOrEmpty(queryText) && (queryEmbedding == null || queryEmbedding.length == 0)) {
                sendResult(request, response,
                        new RestBaseResult("\"query_text\" or \"query_embedding\" is required"));
                return;
            }

            @SuppressWarnings("unchecked")
            List<String> collections = (List<String>) payload.get("collections");
            String collectionType = (String) payload.get("collection_type");
            ContextScopeResolver.ResolvedScope resolvedScope = ContextScopeResolver.resolve(
                    GlobalStateMgr.getCurrentState().getContextMgr(),
                    (String) payload.get("scope"),
                    (String) payload.get("contextbase"),
                    (String) payload.get("collection"),
                    collections,
                    collectionType);
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), resolvedScope.contextBase,
                    ContextRestAuth.BaseAction.USAGE);

            VectorSearchExecutor.Request req = new VectorSearchExecutor.Request();
            req.queryText = queryText;
            req.queryEmbedding = queryEmbedding;
            req.contextBaseId = resolvedScope.contextBaseId;
            req.collectionId = resolvedScope.collectionId;
            req.collectionIds = resolvedScope.collectionIds;
            req.entityType = (String) payload.get("entity_type");
            req.maxResults = ((Number) payload.getOrDefault("limit", 10L)).intValue();
            req.offset = ((Number) payload.getOrDefault("offset", 0L)).intValue();
            req.allowStaleVector = Boolean.TRUE.equals(payload.get("allow_stale_vector"));
            Number confidenceMin = (Number) payload.get("confidence_min");
            if (confidenceMin != null) {
                req.confidenceMin = confidenceMin.doubleValue();
            }
            applyOptions(req, (String) payload.get("options"));
            // Optional explicit fragment selector: "preview" | "section" | "both" (default both).
            // Overrides the -d option. Lets callers pin retrieval granularity, e.g. to A/B
            // preview-only against preview+section recall.
            Object fragment = payload.get("fragment");
            if (fragment instanceof String && !Strings.isNullOrEmpty((String) fragment)) {
                req.fragmentMode = (String) fragment;
            }
            req.snapshotFence = resolveFence(resolvedScope.contextBaseId, payload);
            if (isAsOfRequested(payload) && req.snapshotFence < 0) {
                // Mirror ContextReadCollectionAction: when caller explicitly asks for an as-of
                // view but no snapshot is visible at that point, fail loudly instead of silently
                // returning current state -- otherwise the time-travel fence is unenforceable.
                sendResult(request, response, new RestBaseResult(
                        "no snapshot visible at as_of_time=" + asOfDescriptor(payload)));
                return;
            }

            List<VectorSearchExecutor.EntityHit> hits = GlobalStateMgr.getCurrentState()
                    .getContextVectorSearchExecutor().search(req);
            List<Long> ids = new ArrayList<>(hits.size());
            for (VectorSearchExecutor.EntityHit hit : hits) {
                ids.add(hit.entityId);
            }
            Map<Long, ContextReadExecutor.EntityMeta> metaById = GlobalStateMgr.getCurrentState()
                    .getContextReadExecutor().loadEntityMetadata(ids, req.snapshotFence);

            SearchResponse resp = new SearchResponse();
            resp.requestId = ContextRestAuth.currentRequestId();
            resp.hits = new ArrayList<>();
            for (VectorSearchExecutor.EntityHit hit : hits) {
                ContextReadExecutor.EntityMeta meta = metaById.get(hit.entityId);
                Hit entry = new Hit();
                entry.id = hit.entityId;
                if (meta != null) {
                    entry.entityKey = meta.entityKey;
                    entry.entityType = meta.entityType;
                    entry.preview = meta.preview;
                    entry.version = meta.version;
                    entry.snapshotVersion = meta.snapshotVersion;
                    entry.confidence = meta.confidence;
                    entry.frontmatterJson = meta.frontmatterJson;
                }
                entry.vectorScore = hit.score;
                entry.matchedFragmentKind = hit.fragmentKind;
                entry.matchedSnippet = hit.snippet;
                resp.hits.add(entry);
            }
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    private void applyOptions(VectorSearchExecutor.Request req, String options) {
        if (Strings.isNullOrEmpty(options)) {
            return;
        }
        for (String token : options.trim().split("\\s+")) {
            if (token.equals("-d")) {
                req.deepMode = true;
            } else if (token.equals("-l")) {
                req.idsOnly = true;
            } else if (token.equals("-f")) {
                req.includeFrontmatter = true;
            }
        }
    }

    private long resolveFence(long contextBaseId, Map<String, Object> payload) {
        Object snap = payload.get("snapshot_version");
        Object asOf = payload.get("as_of_time");
        if (snap == null && (asOf == null || (asOf instanceof String && ((String) asOf).isEmpty()))) {
            return -1L;
        }
        com.starrocks.context.SnapshotResolver resolver =
                GlobalStateMgr.getCurrentState().getContextSnapshotResolver();
        if (snap != null) {
            return resolver.resolveFromSelector(contextBaseId, snap.toString());
        }
        return resolver.resolveFromSelector(contextBaseId, asOf.toString());
    }

    private static boolean isAsOfRequested(Map<String, Object> payload) {
        Object snap = payload.get("snapshot_version");
        Object asOf = payload.get("as_of_time");
        if (snap != null) {
            return true;
        }
        return asOf instanceof String && !((String) asOf).isEmpty();
    }

    private static String asOfDescriptor(Map<String, Object> payload) {
        Object snap = payload.get("snapshot_version");
        if (snap != null) {
            return snap.toString();
        }
        Object asOf = payload.get("as_of_time");
        return asOf == null ? "" : asOf.toString();
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

    private static final class SearchResponse {
        @JsonProperty("request_id")
        public String requestId;
        public List<Hit> hits;
    }

    private static final class Hit {
        public long id;

        @JsonProperty("entity_key")
        public String entityKey;

        @JsonProperty("entity_type")
        public String entityType;

        public String preview;
        public long version;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;

        public double confidence;

        @JsonProperty("vector_score")
        public double vectorScore;

        @JsonProperty("matched_fragment_kind")
        public String matchedFragmentKind;

        @JsonProperty("matched_snippet")
        public String matchedSnippet;

        // Verbatim frontmatter JSON for the entity's matched version. Lets clients recover the
        // ingest-time `path` (and any other frontmatter keys) without a follow-up
        // /api/context/get round-trip per hit.
        @JsonProperty("frontmatter_json")
        public String frontmatterJson;
    }
}
