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
import com.starrocks.context.ContextReadExecutor;
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
import com.starrocks.sql.parser.NodePosition;
import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/bulk-delete}. Symmetric to {@link ContextBulkImportAction}. Body:
 * <pre>{@code
 * {
 *   "contextbase": "sales_ai",
 *   "collection": "pipeline_rules",
 *   "selectors": [
 *     {"id": 301},
 *     {"entity_key": "smb_baseline"},
 *     {"id": 302, "entity_key": "deal_scoring.rule"}
 *   ]
 * }
 * }</pre>
 *
 * <p>Each row is independently tombstoned. Selectors carrying just {@code entity_key} are resolved
 * to {@code id} via {@link ContextReadExecutor#resolveEntityIdByKey}; rows that fail resolution are
 * captured in the response with a clear error rather than aborting the batch.
 */
public class ContextBulkDeleteAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextBulkDeleteAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/bulk-delete",
                new ContextBulkDeleteAction(controller));
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
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> selectors = (List<Map<String, Object>>) payload.get("selectors");
            if (selectors == null || selectors.isEmpty()) {
                sendResult(request, response,
                        new RestBaseResult("\"selectors\" must be a non-empty array"));
                return;
            }

            ContextCollectionName name = new ContextCollectionName(contextBase, collection, NodePosition.ZERO);
            ContextWriteExecutor writer = GlobalStateMgr.getCurrentState().getContextWriteExecutor();
            ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();
            ContextCommandService service = new ContextCommandService(reader, writer);
            // Resolve scope ids once so the per-selector entity_key lookup is collection-scoped.
            // Bulk-delete operates on a single collection so ambiguous lookups must not silently
            // route to a sibling collection / base.
            com.starrocks.context.ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            com.starrocks.context.ContextMgr.ContextBaseMeta cb = mgr.getContextBase(contextBase);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult(
                        "contextbase not found: " + contextBase));
                return;
            }
            Long collectionId = null;
            for (com.starrocks.context.ContextMgr.CollectionMeta col : mgr.listCollections(contextBase)) {
                if (collection.equals(col.getName())) {
                    collectionId = col.getId();
                    break;
                }
            }
            if (collectionId == null) {
                sendResult(request, response, new RestBaseResult(
                        "collection not found: " + contextBase + "." + collection));
                return;
            }

            BulkResponse resp = new BulkResponse();
            resp.results = new ArrayList<>();
            int okCount = 0;
            int failCount = 0;
            for (int i = 0; i < selectors.size(); i++) {
                Map<String, Object> sel = selectors.get(i);
                DeleteRow row = new DeleteRow();
                row.index = i;
                try {
                    Number idNum = (Number) sel.get("id");
                    String entityKey = (String) sel.get("entity_key");
                    long id;
                    if (idNum != null) {
                        id = idNum.longValue();
                    } else if (!Strings.isNullOrEmpty(entityKey)) {
                        id = reader.resolveEntityIdByKey(entityKey, cb.getId(), collectionId);
                        if (id < 0) {
                            row.ok = false;
                            row.error = "entity_key not found in scope: " + entityKey;
                            failCount++;
                            resp.results.add(row);
                            continue;
                        }
                    } else {
                        row.ok = false;
                        row.error = "selector requires either \"id\" or \"entity_key\"";
                        failCount++;
                        resp.results.add(row);
                        continue;
                    }
                    ContextWriteExecutor.UpsertResult result = service.delete(name,
                            id, entityKey, false, null);
                    row.id = result.entityId;
                    row.tombstoneVersion = result.version;
                    row.snapshotVersion = result.snapshotVersion;
                    row.ok = true;
                    okCount++;
                } catch (Exception e) {
                    row.ok = false;
                    row.error = e.getMessage();
                    failCount++;
                }
                resp.results.add(row);
            }
            resp.deleted = okCount;
            resp.failed = failCount;
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }

    private static final class BulkResponse {
        public int deleted;
        public int failed;
        public List<DeleteRow> results;
    }

    private static final class DeleteRow {
        public int index;
        public long id;

        @JsonProperty("tombstone_version")
        public long tombstoneVersion;

        @JsonProperty("snapshot_version")
        public long snapshotVersion;

        public boolean ok;
        public String error;
    }
}
