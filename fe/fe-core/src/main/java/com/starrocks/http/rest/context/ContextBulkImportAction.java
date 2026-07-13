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
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextWriteExecutor;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@code POST /api/context/bulk-import}. Body:
 * <pre>{@code
 * {
 *   "contextbase": "sales_ai",
 *   "collection": "pipeline_rules",
 *   "entities": [ {...entity...}, {...entity...} ]
 * }
 * }</pre>
 * Upserts every entity in the list, collecting per-row results. The response contains one
 * {@code results[i]} per input entity; failures are captured inline so a single bad entity does
 * not abort the batch.
 */
public class ContextBulkImportAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(ContextBulkImportAction.class);
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextBulkImportAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/bulk-import",
                new ContextBulkImportAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        Stopwatch swAll = Stopwatch.createStarted();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            String body = request.getContent();
            if (Strings.isNullOrEmpty(body)) {
                sendResult(request, response, new RestBaseResult("body is required"));
                return;
            }
            Map<String, Object> payload = GSON.fromJson(body, MAP_TYPE);
            long parseUs = sw.elapsed(TimeUnit.MICROSECONDS);
            sw.reset().start();
            String contextBase = (String) payload.get("contextbase");
            String collection = (String) payload.get("collection");
            if (Strings.isNullOrEmpty(contextBase) || Strings.isNullOrEmpty(collection)) {
                sendResult(request, response,
                        new RestBaseResult("\"contextbase\" and \"collection\" are required"));
                return;
            }
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase, ContextRestAuth.BaseAction.USAGE);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> entities = (List<Map<String, Object>>) payload.get("entities");
            if (entities == null || entities.isEmpty()) {
                sendResult(request, response, new RestBaseResult("\"entities\" must be a non-empty array"));
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map<String, Object>) payload.get("options");

            ContextCollectionName name = new ContextCollectionName(contextBase, collection, NodePosition.ZERO);
            ContextWriteExecutor writer = GlobalStateMgr.getCurrentState().getContextWriteExecutor();
            Map<String, Expr> optionExprs = toExprMap(options);

            // Build the per-row arg list once. The batched upsert path issues at most 5
            // multi-row INSERTs regardless of N (instead of 5×N round-trips in the legacy
            // per-row loop). Per-row error isolation is preserved server-side: each entity is
            // pre-validated on FE before any SQL fires, so a single bad row only fails its own
            // slot.
            // Per-entity explicit edges. Mirrors the single /upsert surface: each entity may carry
            // an `edges: [{dst_entity_key|dst_entity_id|dst}]` array. We parse it via the shared
            // ContextUpsertAction.toEdgeExprs and pass a parallel-indexed perEntityEdges list to
            // upsertBatch, which is forward-reference-safe (unresolved keys are kept with
            // dst_entity_id=0 + dst_entity_key and resolved at read time). The `edges` key is
            // stripped from the entity args so toExprMap doesn't serialize the list as a bogus arg.
            List<List<Expr>> perEntityEdges = extractPerEntityEdges(entities);
            List<Map<String, Expr>> argsList = new ArrayList<>(entities.size());
            for (Map<String, Object> ent : entities) {
                if (ent.containsKey("edges")) {
                    Map<String, Object> entArgs = new java.util.LinkedHashMap<>(ent);
                    entArgs.remove("edges");
                    argsList.add(toExprMap(entArgs));
                } else {
                    argsList.add(toExprMap(ent));
                }
            }
            long toExprUs = sw.elapsed(TimeUnit.MICROSECONDS);
            sw.reset().start();
            LOG.info("bulkImport begin n={} body_bytes={} parse_us={} toExpr_us={}",
                    entities.size(), body.length(), parseUs, toExprUs);
            List<ContextWriteExecutor.UpsertOutcome> outcomes;
            try {
                outcomes = writer.upsertBatch(name, argsList, perEntityEdges, optionExprs);
            } catch (com.starrocks.context.error.ContextException e) {
                // Structured pre-flight failure (e.g. embedding provider not configured) — let it
                // propagate to the outer catch so the caller gets ContextErrorResult shape with a
                // stable error_code, not a generic RestBaseResult.
                throw e;
            } catch (Exception e) {
                // Other pre-flight failures (bad collection / contextbase) — no partial result.
                sendResult(request, response, new RestBaseResult(e.getMessage()));
                return;
            }
            long upsertMs = sw.elapsed(TimeUnit.MILLISECONDS);
            sw.reset().start();

            BulkResponse resp = new BulkResponse();
            resp.results = new ArrayList<>(outcomes.size());
            int okCount = 0;
            int failCount = 0;
            for (ContextWriteExecutor.UpsertOutcome outcome : outcomes) {
                EntityResult row = new EntityResult();
                row.index = outcome.index;
                row.ok = outcome.ok;
                if (outcome.ok && outcome.result != null) {
                    row.id = outcome.result.entityId;
                    row.version = outcome.result.version;
                    row.snapshotVersion = outcome.result.snapshotVersion;
                    okCount++;
                } else {
                    row.error = outcome.errorMessage;
                    failCount++;
                }
                resp.results.add(row);
            }
            resp.imported = okCount;
            resp.failed = failCount;
            LOG.info("bulkImport done upsert_ms={} build_resp_us={} total_ms={} ok={} fail={}",
                    upsertMs, sw.elapsed(TimeUnit.MICROSECONDS),
                    swAll.elapsed(TimeUnit.MILLISECONDS), okCount, failCount);
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }

    private static Map<String, Expr> toExprMap(Map<String, Object> in) {
        if (in == null) {
            return null;
        }
        Map<String, Expr> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : in.entrySet()) {
            Object value = entry.getValue();
            String str = value == null ? null : value.toString();
            out.put(entry.getKey(), new StringLiteral(str == null ? "" : str));
        }
        return out;
    }

    /**
     * Parse each entity's optional {@code edges} array into the parallel-indexed perEntityEdges
     * list that {@link ContextWriteExecutor#upsertBatch} consumes (index i = edges of entity i).
     * Reuses {@link ContextUpsertAction#toEdgeExprs}. Returns {@code null} when no entity carries
     * edges, which upsertBatch treats as "no edges anywhere" (cheap fast path). Package-private
     * for unit testing. The {@code edges} key is left on the entity map here; the caller strips it
     * from the args it passes to {@code toExprMap}.
     */
    static List<List<Expr>> extractPerEntityEdges(List<Map<String, Object>> entities) {
        List<List<Expr>> perEntityEdges = new ArrayList<>(entities.size());
        boolean anyEdges = false;
        for (Map<String, Object> ent : entities) {
            Object edgesRaw = ent == null ? null : ent.get("edges");
            List<Expr> edgeExprs = null;
            if (edgesRaw instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> edgeList = (List<Map<String, Object>>) edgesRaw;
                edgeExprs = ContextUpsertAction.toEdgeExprs(edgeList);
                if (edgeExprs != null && !edgeExprs.isEmpty()) {
                    anyEdges = true;
                }
            }
            perEntityEdges.add(edgeExprs);
        }
        return anyEdges ? perEntityEdges : null;
    }

    private static final class BulkResponse {
        public int imported;
        public int failed;
        public List<EntityResult> results;
    }

    private static final class EntityResult {
        public int index;
        public long id;
        public long version;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;

        public boolean ok;
        public String error;
    }
}
