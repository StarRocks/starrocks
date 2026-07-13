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
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.parser.NodePosition;
import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * {@code POST /api/context/update-metadata}. Replaces an entity's {@code frontmatter_json}
 * wholesale WITHOUT re-embedding the body — the cheap path for high-frequency metadata writes
 * (e.g. verify-on-use staleness state) that live in frontmatter but do not change the embedded
 * text.
 *
 * <p>Request: {@code {contextbase, collection, entity_key | id, frontmatter:{...}}}. The
 * {@code frontmatter} object is serialized and stored as-is — arbitrary keys are accepted (no
 * whitelist) and the value replaces the prior frontmatter completely (no per-key merge).
 */
public class ContextUpdateMetadataAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextUpdateMetadataAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/update-metadata",
                new ContextUpdateMetadataAction(controller));
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

            Number idNum = (Number) payload.get("id");
            String entityKey = (String) payload.get("entity_key");
            if (idNum == null && Strings.isNullOrEmpty(entityKey)) {
                sendResult(request, response,
                        new RestBaseResult("one of \"id\"/\"entity_key\" is required"));
                return;
            }
            Object frontmatter = payload.get("frontmatter");
            if (frontmatter == null) {
                sendResult(request, response, new RestBaseResult("\"frontmatter\" is required"));
                return;
            }
            String frontmatterJson = GSON.toJson(frontmatter);

            ContextWriteExecutor.UpsertResult result = GlobalStateMgr.getCurrentState()
                    .getContextWriteExecutor()
                    .updateMetadata(name, idNum == null ? null : idNum.longValue(), entityKey, frontmatterJson);

            UpdateMetadataResponse resp = new UpdateMetadataResponse();
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

    private static class UpdateMetadataResponse {
        public long id;
        public long version;
        @JsonProperty("snapshot_version")
        public long snapshotVersion;
        @JsonProperty("entity_key")
        public String entityKey;
    }
}
