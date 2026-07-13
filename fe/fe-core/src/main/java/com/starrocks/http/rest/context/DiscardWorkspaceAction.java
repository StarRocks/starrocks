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
import com.starrocks.context.ContextMgr;
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
 * {@code POST /api/workspaces/discard}. Tombstones the workspace's latest active objects and then
 * removes the workspace metadata. The shared collections are unaffected.
 *
 * <p>Body: {@code {"qualified_name": "<cb.col.ws>"}}.
 */
public class DiscardWorkspaceAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public DiscardWorkspaceAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/workspaces/discard", new DiscardWorkspaceAction(controller));
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
            String qualifiedName = (String) payload.get("qualified_name");
            if (Strings.isNullOrEmpty(qualifiedName)) {
                sendResult(request, response, new RestBaseResult("\"qualified_name\" is required"));
                return;
            }
            int firstDot = qualifiedName.indexOf('.');
            String contextBase = firstDot > 0 ? qualifiedName.substring(0, firstDot) : null;
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase, ContextRestAuth.BaseAction.USAGE);

            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextMgr.WorkspaceMeta workspace = mgr.getWorkspace(qualifiedName);
            if (workspace == null) {
                sendResult(request, response, new RestBaseResult("workspace not found: " + qualifiedName));
                return;
            }

            WorkspaceObjectWriter writer = GlobalStateMgr.getCurrentState().getWorkspaceObjectWriter();
            for (WorkspaceRestSupport.WorkspaceObjectRef ref : WorkspaceRestSupport.listActiveObjects(workspace)) {
                writer.discard(workspace, ref.objectId, ref.workspaceScope);
            }
            mgr.dropWorkspace(qualifiedName, true);

            DiscardResponse resp = new DiscardResponse();
            resp.qualifiedName = qualifiedName;
            resp.workspaceId = workspace.getId();
            resp.discarded = true;
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

    private static final class DiscardResponse {
        @JsonProperty("qualified_name")
        public String qualifiedName;

        @JsonProperty("workspace_id")
        public long workspaceId;

        public boolean discarded;
    }
}
