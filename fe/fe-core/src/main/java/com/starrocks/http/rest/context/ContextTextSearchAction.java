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
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.retrieval.ContextScopeResolver;
import com.starrocks.context.retrieval.TextSearchExecutor;
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
 * {@code POST /api/context/text-search}. Body:
 * {@code {"contextbase": "...", "collection": "...", "pattern": "...", "entity_type": "page", "limit": 10}}.
 */
public class ContextTextSearchAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextTextSearchAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/text-search",
                new ContextTextSearchAction(controller));
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
            String pattern = (String) payload.get("pattern");
            if (Strings.isNullOrEmpty(pattern)) {
                sendResult(request, response, new RestBaseResult("\"pattern\" is required"));
                return;
            }
            String contextBase = (String) payload.get("contextbase");
            String collection = (String) payload.get("collection");
            String scope = (String) payload.get("scope");
            @SuppressWarnings("unchecked")
            List<String> collections = (List<String>) payload.get("collections");
            String collectionType = (String) payload.get("collection_type");
            ContextScopeResolver.ResolvedScope resolvedScope = ContextScopeResolver.resolve(
                    GlobalStateMgr.getCurrentState().getContextMgr(), scope, contextBase, collection,
                    collections, collectionType);
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), resolvedScope.contextBase,
                    ContextRestAuth.BaseAction.USAGE);

            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextMgr.ContextBaseMeta cb = mgr.getContextBase(resolvedScope.contextBase);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult(
                        "contextbase not found: " + resolvedScope.contextBase));
                return;
            }

            TextSearchExecutor.Request req = new TextSearchExecutor.Request();
            req.pattern = pattern;
            req.entityType = (String) payload.get("entity_type");
            req.maxResults = ((Number) payload.getOrDefault("limit", 10L)).intValue();
            req.offset = ((Number) payload.getOrDefault("offset", 0L)).intValue();
            req.contextBaseId = cb.getId();
            req.collectionId = resolvedScope.collectionId;
            req.collectionIds = resolvedScope.collectionIds;
            // grep-style options per API doc §7.5.
            String options = (String) payload.get("options");
            applyOptions(req, options);
            Number conf = (Number) payload.get("confidence_min");
            if (conf != null) {
                req.confidenceMin = conf.doubleValue();
            }
            // Snapshot fence — explicit snapshot_version or as_of_time string.
            req.snapshotFence = resolveFence(cb.getId(), payload);
            if (isAsOfRequested(payload) && req.snapshotFence < 0) {
                // Mirror ContextReadCollectionAction: when caller explicitly asks for an as-of
                // view but no snapshot is visible at that point, fail loudly instead of silently
                // returning current state.
                sendResult(request, response, new RestBaseResult(
                        "no snapshot visible at as_of_time=" + asOfDescriptor(payload)));
                return;
            }

            List<TextSearchExecutor.EntityHit> hits = GlobalStateMgr.getCurrentState()
                    .getContextTextSearchExecutor().search(req);
            List<Long> ids = new ArrayList<>(hits.size());
            for (TextSearchExecutor.EntityHit hit : hits) {
                ids.add(hit.entityId);
            }
            Map<Long, ContextReadExecutor.EntityMeta> metaById = GlobalStateMgr.getCurrentState()
                    .getContextReadExecutor().loadEntityMetadata(ids, req.snapshotFence);

            SearchResponse resp = new SearchResponse();
            resp.hits = new ArrayList<>();
            for (TextSearchExecutor.EntityHit h : hits) {
                ContextReadExecutor.EntityMeta meta = metaById.get(h.entityId);
                Hit entry = new Hit();
                entry.id = h.entityId;
                if (meta != null) {
                    entry.entityKey = meta.entityKey;
                    entry.entityType = meta.entityType;
                    entry.preview = meta.preview;
                    entry.version = meta.version;
                    entry.snapshotVersion = meta.snapshotVersion;
                    entry.confidence = meta.confidence;
                    entry.frontmatterJson = meta.frontmatterJson;
                }
                entry.hitCount = h.hitCount;
                entry.textScore = h.textScore;
                if (h.topSnippet != null) {
                    entry.topSnippet = h.topSnippet.snippet;
                    entry.snippetFragmentKind = h.topSnippet.fragmentKind;
                    entry.lineStart = h.topSnippet.lineStart;
                    entry.lineEnd = h.topSnippet.lineEnd;
                }
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

    /**
     * Parse a grep-style {@code options} string (e.g. {@code "-i -C 3"}) into the request flags.
     * Unknown flags are ignored — the spec only requires the documented subset
     * ({@code -i -n -c -l -A -B -C}). Whitespace separates tokens; numeric arguments may either
     * follow the flag in the next token or be glued to it ({@code -C3}).
     */
    private void applyOptions(TextSearchExecutor.Request req, String options) {
        if (Strings.isNullOrEmpty(options)) {
            return;
        }
        String[] tokens = options.trim().split("\\s+");
        for (int i = 0; i < tokens.length; i++) {
            String t = tokens[i];
            if (t.equals("-i")) {
                req.caseInsensitive = true;
            } else if (t.equals("-n")) {
                // line numbers are returned by default; this flag is a no-op kept for compatibility
                continue;
            } else if (t.equals("-c")) {
                req.countOnly = true;
            } else if (t.equals("-l")) {
                req.filenamesOnly = true;
            } else if (t.startsWith("-A") || t.startsWith("-B") || t.startsWith("-C")) {
                String numStr = t.length() > 2 ? t.substring(2) : (i + 1 < tokens.length ? tokens[++i] : "");
                int n;
                try {
                    n = Integer.parseInt(numStr);
                } catch (NumberFormatException e) {
                    continue;
                }
                if (t.startsWith("-A")) {
                    req.afterLines = n;
                } else if (t.startsWith("-B")) {
                    req.beforeLines = n;
                } else {
                    req.contextLines = n;
                }
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

    private static final class SearchResponse {
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

        @JsonProperty("hit_count")
        public int hitCount;

        @JsonProperty("text_score")
        public double textScore;

        @JsonProperty("top_snippet")
        public String topSnippet;

        @JsonProperty("snippet_fragment_kind")
        public String snippetFragmentKind;

        @JsonProperty("line_start")
        public int lineStart;

        @JsonProperty("line_end")
        public int lineEnd;

        // Verbatim frontmatter JSON for the entity's matched version. Lets clients recover the
        // ingest-time `path` (and any other frontmatter keys) without a follow-up
        // /api/context/get round-trip per hit.
        @JsonProperty("frontmatter_json")
        public String frontmatterJson;
    }
}
