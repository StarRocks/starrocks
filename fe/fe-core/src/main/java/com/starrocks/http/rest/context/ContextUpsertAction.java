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
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextWriteExecutor;
import com.starrocks.context.service.ContextCommandService;
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
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/upsert}. Supports both canonical upsert and write-style updates.
 */
public class ContextUpsertAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextUpsertAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/upsert", new ContextUpsertAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        try {
            String body = request.getContent();
            if (Strings.isNullOrEmpty(body)) {
                sendResult(request, response, new RestBaseResult("body is required"));
                return;
            }
            Map<String, Object> payload = GSON.fromJson(body, MAP_TYPE);
            String contextBase = (String) payload.get("contextbase");
            String collection = (String) payload.get("collection");
            if (Strings.isNullOrEmpty(contextBase) || Strings.isNullOrEmpty(collection)) {
                sendResult(request, response,
                        new RestBaseResult("\"contextbase\" and \"collection\" are required"));
                return;
            }
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase, ContextRestAuth.BaseAction.USAGE);
            ContextCollectionName name = new ContextCollectionName(contextBase, collection, NodePosition.ZERO);

            @SuppressWarnings("unchecked")
            Map<String, Object> entity = (Map<String, Object>) payload.get("entity");
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map<String, Object>) payload.get("options");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> edgesRaw = (List<Map<String, Object>>) payload.get("edges");

            ContextWriteExecutor.UpsertResult result;
            if (entity != null && !entity.isEmpty()) {
                result = GlobalStateMgr.getCurrentState().getContextWriteExecutor()
                        .upsert(name, toExprMap(entity), toEdgeExprs(edgesRaw), toExprMap(options));
            } else {
                ContextCommandService service = new ContextCommandService(
                        GlobalStateMgr.getCurrentState().getContextReadExecutor(),
                        GlobalStateMgr.getCurrentState().getContextWriteExecutor());
                Number idNum = (Number) payload.get("id");
                String entityKey = (String) payload.get("entity_key");
                String content = firstNonEmpty((String) payload.get("content"), (String) payload.get("body"));
                String writeOptions = (String) payload.get("write_options");
                String title = (String) payload.get("title");
                String preview = (String) payload.get("preview");
                Number confidenceNum = (Number) payload.get("confidence");
                Double confidence = confidenceNum == null ? null : confidenceNum.doubleValue();
                boolean deprecate = Boolean.TRUE.equals(payload.get("deprecate"));
                if (idNum == null && Strings.isNullOrEmpty(entityKey)) {
                    sendResult(request, response,
                            new RestBaseResult("\"entity\" or one of \"id\"/\"entity_key\" is required"));
                    return;
                }
                if (deprecate) {
                    result = service.deprecate(name, idNum == null ? null : idNum.longValue(), entityKey, toExprMap(options));
                } else {
                    result = service.write(name, idNum == null ? null : idNum.longValue(), entityKey,
                            content, writeOptions, title, preview, confidence, toExprMap(options));
                }
            }

            UpsertResponse resp = new UpsertResponse();
            resp.id = result.entityId;
            resp.version = result.version;
            resp.snapshotVersion = result.snapshotVersion;
            resp.entityKey = result.entityKey;
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response, ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalStateException | IllegalArgumentException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    private static Map<String, Expr> toExprMap(Map<String, Object> in) {
        if (in == null) {
            return null;
        }
        Map<String, Expr> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : in.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (value instanceof Number number) {
                if (value instanceof Float || value instanceof Double) {
                    out.put(entry.getKey(), new FloatLiteral(number.doubleValue()));
                } else {
                    out.put(entry.getKey(), new IntLiteral(number.longValue()));
                }
            } else {
                out.put(entry.getKey(), new StringLiteral(value.toString()));
            }
        }
        return out;
    }

    private static String firstNonEmpty(String first, String second) {
        return !Strings.isNullOrEmpty(first) ? first : second;
    }

    /**
     * Convert the JSON {@code edges} array into the {@code List<Expr>} shape that
     * {@link ContextWriteExecutor#upsert(ContextCollectionName, Map, List, Map)} consumes. The
     * SQL {@code EDGES (...)} clause accepts either an {@link IntLiteral} (destination entity id)
     * or a {@link StringLiteral} (destination entity_key, resolved through the heads table at
     * write time — see {@code ContextWriteExecutor.resolveEdgeList}). The JSON surface mirrors
     * both forms and adds a {@code "dst"} shorthand that auto-detects which Expr to emit.
     *
     * <p>All edges are persisted with {@code ref_kind='explicit'}, matching the SQL clause's
     * semantics. The {@code ref_kind} / {@code ref_label} fields on each edge JSON entry are
     * accepted for forward compatibility but currently ignored — extending per-edge ref_kind
     * would require a richer Expr (e.g. {@code FunctionCallExpr edge('key','kind')}) at the
     * writer layer.
     */
    // Package-private so unit tests in the same package can pin the JSON-to-Expr mapping
    // without spinning up the full HTTP / write-executor stack.
    static List<Expr> toEdgeExprs(List<Map<String, Object>> in) {
        if (in == null || in.isEmpty()) {
            return null;
        }
        List<Expr> out = new ArrayList<>(in.size());
        for (Map<String, Object> edge : in) {
            if (edge == null) {
                continue;
            }
            Object id = edge.get("dst_entity_id");
            Object key = edge.get("dst_entity_key");
            if (id == null && key == null) {
                // Convenience shorthand: a single "dst" field whose type drives interpretation.
                Object dst = edge.get("dst");
                if (dst instanceof Number) {
                    id = dst;
                } else if (dst instanceof String) {
                    key = dst;
                }
            }
            if (id instanceof Number) {
                out.add(new IntLiteral(((Number) id).longValue()));
            } else if (key instanceof String && !Strings.isNullOrEmpty((String) key)) {
                out.add(new StringLiteral((String) key));
            }
            // Malformed entries (no dst at all) are silently dropped. The writer logs WARN
            // on any unresolved edge so partial drops still leave an audit trail.
        }
        return out.isEmpty() ? null : out;
    }

    private static final class UpsertResponse {
        public long id;
        public long version;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;

        @JsonProperty("entity_key")
        public String entityKey;
    }
}
