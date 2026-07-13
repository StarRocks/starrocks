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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/graph-expand}. External name is kept for compatibility with the
 * architecture doc's {@code GRAPH_EXPAND} contract; the implementation is reference expansion over
 * an ordinary PK table, not a graph engine.
 */
public class ContextGraphExpandAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextGraphExpandAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/graph-expand",
                new ContextGraphExpandAction(controller));
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
            // The user-facing API contract uses `seed_ids` (api/sql/tvf design §4.5.4); accept
            // it as the canonical name and keep `seeds` as a back-compat alias.
            @SuppressWarnings("unchecked")
            List<Number> seedList = (List<Number>) payload.get("seed_ids");
            if (seedList == null) {
                @SuppressWarnings("unchecked")
                List<Number> alias = (List<Number>) payload.get("seeds");
                seedList = alias;
            }
            if (seedList == null || seedList.isEmpty()) {
                sendResult(request, response, new RestBaseResult("\"seed_ids\" is required"));
                return;
            }

            String contextBase = (String) payload.get("contextbase");
            String collection = (String) payload.get("collection");
            String scope = (String) payload.get("scope");
            @SuppressWarnings("unchecked")
            List<String> collections = (List<String>) payload.get("collections");
            String collectionType = (String) payload.get("collection_type");
            if (Strings.isNullOrEmpty(contextBase) && Strings.isNullOrEmpty(scope)) {
                sendResult(request, response, new RestBaseResult("\"contextbase\" or \"scope\" is required"));
                return;
            }
            // Privilege gate. Same shape the SQL surface uses — caller must hold USAGE on the
            // contextbase or have an admin override. Refusing without any scope blocks the
            // cluster-wide expansion that would otherwise leak edges across bases.
            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextScopeResolver.ResolvedScope resolvedScope = ContextScopeResolver.resolve(
                    mgr, scope, contextBase, collection, collections, collectionType);
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), resolvedScope.contextBase,
                    ContextRestAuth.BaseAction.USAGE);
            ContextMgr.ContextBaseMeta cb = mgr.getContextBase(resolvedScope.contextBase);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult("contextbase not found: " + resolvedScope.contextBase));
                return;
            }
            Long collectionId = resolvedScope.collectionId;
            List<Long> collectionIds = resolvedScope.collectionIds;

            ReferenceExpander.Request req = new ReferenceExpander.Request();
            List<Long> seeds = new ArrayList<>(seedList.size());
            for (Number n : seedList) {
                seeds.add(n.longValue());
            }
            // Validate that every seed lives in the named contextbase. Without this check, a
            // caller with USAGE on base A could pass seed_ids from base B and (depending on
            // expander filtering) leak edges/rows across tenants — same shape as the membership
            // check in ContextPackAction.
            long mismatched = countMismatchedSeeds(cb.getId(), seeds);
            if (mismatched < 0) {
                sendResult(request, response, new RestBaseResult(
                        "cannot verify seed membership against contextbase " + resolvedScope.contextBase));
                return;
            }
            if (mismatched > 0) {
                sendResult(request, response, new RestBaseResult(
                        mismatched + " seed(s) do not belong to contextbase " + resolvedScope.contextBase));
                return;
            }
            req.seeds = seeds;
            String direction = (String) payload.getOrDefault("direction", "FORWARD");
            req.direction = ReferenceExpander.Direction.valueOf(direction.toUpperCase());
            // `max_depth` is the documented field; `depth` is kept as a legacy alias.
            Number depthArg = (Number) payload.get("max_depth");
            if (depthArg == null) {
                depthArg = (Number) payload.getOrDefault("depth", 1L);
            }
            req.depth = depthArg.intValue();
            req.maxFrontier = ((Number) payload.getOrDefault("max_frontier", 200L)).intValue();
            @SuppressWarnings("unchecked")
            List<String> edgeTypes = (List<String>) payload.get("edge_types");
            req.refKinds = edgeTypes;
            req.requireComplete = Boolean.TRUE.equals(payload.get("require_complete"));
            req.contextBaseId = cb.getId();
            req.collectionId = collectionId;
            req.collectionIds = collectionIds;
            // Snapshot fence: caller may pin a specific snapshot_version or an as_of_time string.
            // The SnapshotResolver translates either into the canonical fence the expander uses.
            req.snapshotFence = resolveFence(cb.getId(), payload);
            if (isAsOfRequested(payload) && req.snapshotFence < 0) {
                // Mirror ContextReadCollectionAction: when caller explicitly asks for an as-of
                // view but no snapshot is visible at that point, fail loudly instead of returning
                // a misleading partial result (seeds-only with no graph context).
                sendResult(request, response, new RestBaseResult(
                        "no snapshot visible at as_of_time=" + asOfDescriptor(payload)));
                return;
            }

            ReferenceExpander.Result result = GlobalStateMgr.getCurrentState()
                    .getContextReferenceExpander().expand(req);

            ExpandResponse resp = new ExpandResponse();
            resp.rows = new ArrayList<>();
            List<Long> ids = new ArrayList<>(result.rows.size());
            for (ReferenceExpander.ExpansionRow row : result.rows) {
                ids.add(row.entityId);
            }
            Map<Long, ContextReadExecutor.EntityMeta> metaById = GlobalStateMgr.getCurrentState()
                    .getContextReadExecutor().loadEntityMetadata(ids, req.snapshotFence);
            for (ReferenceExpander.ExpansionRow row : result.rows) {
                ContextReadExecutor.EntityMeta meta = metaById.get(row.entityId);
                Row r = new Row();
                r.seedId = row.seedId;
                r.id = row.entityId;
                r.entityKey = meta == null ? null : meta.entityKey;
                r.hop = row.hop;
                r.pathScore = row.pathScore;
                r.edgeTypes = row.refKinds;
                r.snapshotVersion = meta == null ? 0L : meta.snapshotVersion;
                java.util.Map<String, Object> pathMeta = new java.util.LinkedHashMap<>();
                pathMeta.put("seed_id", row.seedId);
                pathMeta.put("hop", row.hop);
                pathMeta.put("edge_types", row.refKinds);
                r.pathMeta = GSON.toJson(pathMeta);
                resp.rows.add(r);
            }
            resp.truncated = result.truncated;
            resp.maxHopReached = result.maxHopReached;
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            // Structured error response per API doc §12.2 — gives clients enough to build
            // retry/backoff logic without parsing free text.
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    private static final class ExpandResponse {
        public List<Row> rows;
        public boolean truncated;

        @JsonProperty("max_hop_reached")
        public int maxHopReached;
    }

    private static final class Row {
        @JsonProperty("seed_id")
        public long seedId;

        public long id;

        @JsonProperty("entity_key")
        public String entityKey;

        public int hop;

        @JsonProperty("path_score")
        public double pathScore;

        @JsonProperty("edge_types")
        public List<String> edgeTypes;

        @JsonProperty("path_meta")
        public String pathMeta;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;
    }

    /**
     * Count how many of the supplied seeds live in a contextbase other than the expected one.
     * Returns -1 when the SELECT can't run (heads not yet materialized) so the caller can refuse
     * the request without leaking. Mirrors ContextPackAction.countMismatchedEntities.
     */
    private long countMismatchedSeeds(long expectedContextBaseId, List<Long> seeds) {
        if (seeds == null || seeds.isEmpty()) {
            return 0;
        }
        StringBuilder inList = new StringBuilder();
        for (int i = 0; i < seeds.size(); i++) {
            if (i > 0) {
                inList.append(',');
            }
            inList.append(seeds.get(i));
        }
        return GlobalStateMgr.getCurrentState().getContextReadExecutor()
                .countWithFilter(com.starrocks.context.ContextInternalTables.HEADS,
                        "entity_id IN (" + inList + ") AND contextbase_id != " + expectedContextBaseId);
    }

    /**
     * Resolve {@code snapshot_version} or {@code as_of_time} from the payload to the canonical
     * snapshot fence the expander uses. Returns -1 when neither is supplied so callers default to
     * "current heads".
     */
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
}
