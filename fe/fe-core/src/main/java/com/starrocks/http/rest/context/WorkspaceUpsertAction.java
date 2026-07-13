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
import com.starrocks.context.WorkspaceObjectWriter;
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
import java.util.Map;

/**
 * {@code POST /api/workspace/upsert}. Body:
 * {@code {"workspace": "<cb.col.ws>", "object_id": "...", "object_type": "...", "workspace_scope": "scratch",
 * "payload": {...}, "priority": 0.9, "ttl_hours": 12}}.
 */
public class WorkspaceUpsertAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public WorkspaceUpsertAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/workspace/upsert", new WorkspaceUpsertAction(controller));
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
            String workspace = (String) payload.get("workspace");
            String objectId = (String) payload.get("object_id");
            if (Strings.isNullOrEmpty(workspace) || Strings.isNullOrEmpty(objectId)) {
                sendResult(request, response,
                        new RestBaseResult("\"workspace\" and \"object_id\" are required"));
                return;
            }
            int firstDot = workspace.indexOf('.');
            String contextBase = firstDot > 0 ? workspace.substring(0, firstDot) : null;
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase, ContextRestAuth.BaseAction.USAGE);
            String objectType = (String) payload.getOrDefault("object_type", "draft");
            String workspaceScope = (String) payload.getOrDefault(
                    "workspace_scope", WorkspaceObjectWriter.WORKSPACE_SCOPE_SCRATCH);
            @SuppressWarnings("unchecked")
            Map<String, Object> objectPayload = (Map<String, Object>) payload.get("payload");
            Number priorityNum = (Number) payload.getOrDefault("priority", 0.5);
            Number ttlNum = (Number) payload.getOrDefault("ttl_hours", 24L);

            WorkspaceObjectWriter.UpsertResult result = GlobalStateMgr.getCurrentState()
                    .getWorkspaceObjectWriter()
                    .upsert(workspace, objectId, objectType, objectPayload,
                            priorityNum.doubleValue(), ttlNum.longValue(), workspaceScope);

            UpsertResponse resp = new UpsertResponse();
            resp.requestId = ContextRestAuth.currentRequestId();
            resp.workspaceId = result.workspaceId;
            resp.objectId = result.objectId;
            resp.version = result.version;
            resp.snapshotVersion = result.snapshotVersion;
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalStateException | IllegalArgumentException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    private static final class UpsertResponse {
        @JsonProperty("request_id")
        public String requestId;
        @JsonProperty("workspace_id")
        public long workspaceId;

        @JsonProperty("object_id")
        public String objectId;

        public long version;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;
    }
}
