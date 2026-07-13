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
import com.google.gson.JsonArray;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/history}. Body: {@code {"id": 123}}. Returns every version row for the
 * entity in descending {@code version} order so clients can walk the timeline of edits / tombstones.
 */
public class ContextHistoryAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextHistoryAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/history", new ContextHistoryAction(controller));
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
            Number idNum = (Number) payload.get("id");
            if (idNum == null) {
                sendResult(request, response, new RestBaseResult("\"id\" is required"));
                return;
            }
            long entityId = idNum.longValue();
            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();
            ContextMgr.ContextBaseMeta cb = ContextRestReadAuth.authorizeHistoryEntity(mgr, reader, entityId);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult("entity not found: " + entityId));
                return;
            }
            JsonArray rows = reader.getHistory(entityId);

            HistoryResponse resp = new HistoryResponse();
            resp.entityId = entityId;
            resp.rowCount = rows.size();
            resp.rows = decodeHistoryRows(rows);
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (com.starrocks.authorization.AccessDeniedException e) {
            // Re-raise so the framework returns 401 with the canonical message.
            throw e;
        } catch (Exception e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    /**
     * Map the {@code getHistory} SELECT result to named fields. Column order from
     * {@link ContextReadExecutor#getHistory}: {@code 0:entity_id, 1:version, 2:snapshot_version,
     * 3:updated_time, 4:deleted, 5:preview, 6:confidence}.
     */
    private List<Map<String, Object>> decodeHistoryRows(JsonArray rows) {
        List<Map<String, Object>> out = new ArrayList<>(rows.size());
        for (com.google.gson.JsonElement el : rows) {
            JsonArray data = el.getAsJsonObject().getAsJsonArray("data");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", data.size() > 0 && !data.get(0).isJsonNull() ? data.get(0).getAsLong() : null);
            row.put("version", data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsLong() : null);
            row.put("snapshot_version",
                    data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsLong() : null);
            row.put("updated_time",
                    data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsString() : null);
            row.put("deleted", data.size() > 4
                    && com.starrocks.context.ContextJsonUtil.parseBool(data.get(4)));
            row.put("preview", data.size() > 5 && !data.get(5).isJsonNull() ? data.get(5).getAsString() : null);
            row.put("confidence",
                    data.size() > 6 && !data.get(6).isJsonNull() ? data.get(6).getAsDouble() : null);
            out.add(row);
        }
        return out;
    }

    private static final class HistoryResponse {
        @JsonProperty("entity_id")
        public long entityId;

        @JsonProperty("row_count")
        public int rowCount;

        public List<Map<String, Object>> rows;
    }
}
