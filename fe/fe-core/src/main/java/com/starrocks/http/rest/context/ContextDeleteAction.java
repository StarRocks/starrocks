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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code POST /api/context/delete}. Supports soft-delete and hard-delete.
 */
public class ContextDeleteAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextDeleteAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/delete", new ContextDeleteAction(controller));
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
            Number idNum = (Number) payload.get("id");
            String entityKey = (String) payload.get("entity_key");
            if (Strings.isNullOrEmpty(contextBase) || Strings.isNullOrEmpty(collection)
                    || (idNum == null && Strings.isNullOrEmpty(entityKey))) {
                sendResult(request, response,
                        new RestBaseResult("\"contextbase\", \"collection\", and one of \"id\"/\"entity_key\" are required"));
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map<String, Object>) payload.get("options");
            boolean hardDelete = Boolean.TRUE.equals(payload.get("hard_delete"));
            // Soft delete is a tombstoned write — USAGE is appropriate. Hard delete is
            // destructive and unrecoverable, so require DROP on the contextbase.
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                    hardDelete ? ContextRestAuth.BaseAction.DROP : ContextRestAuth.BaseAction.USAGE);
            ContextCollectionName name = new ContextCollectionName(contextBase, collection, NodePosition.ZERO);
            ContextCommandService service = new ContextCommandService(
                    GlobalStateMgr.getCurrentState().getContextReadExecutor(),
                    GlobalStateMgr.getCurrentState().getContextWriteExecutor());

            ContextWriteExecutor.UpsertResult result = service.delete(name,
                    idNum == null ? null : idNum.longValue(), entityKey, hardDelete, toExprMap(options));

            DeleteResponse resp = new DeleteResponse();
            resp.id = result.entityId;
            resp.version = result.version;
            resp.snapshotVersion = result.snapshotVersion;
            resp.deleted = true;
            resp.hardDelete = hardDelete;
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

    private static final class DeleteResponse {
        public long id;
        public long version;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;

        public boolean deleted;

        @JsonProperty("hard_delete")
        public boolean hardDelete;
    }
}
