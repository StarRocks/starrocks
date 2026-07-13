// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
import com.starrocks.context.service.ContextQueryService;
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
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code POST /api/context/get}. Supports point reads, history reads, line-range reads, and
 * progressive-disclosure neighbor expansion.
 */
public class ContextGetAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextGetAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/get", new ContextGetAction(controller));
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
            ContextQueryService.ReadRequest readRequest = new ContextQueryService.ReadRequest();
            Number idNum = (Number) payload.get("id");
            readRequest.id = idNum == null ? null : idNum.longValue();
            readRequest.entityKey = firstNonEmpty((String) payload.get("entity_key"), (String) payload.get("entity_id"));
            Number versionNum = (Number) payload.get("version");
            readRequest.version = versionNum == null ? null : versionNum.longValue();
            readRequest.asOfTime = (String) payload.get("as_of_time");
            readRequest.contextBase = (String) payload.get("contextbase");
            readRequest.collection = (String) payload.get("collection");
            readRequest.level = ContextReadExecutor.DisclosureLevel.parse((String) payload.get("level"));
            Number neighborLimit = (Number) payload.getOrDefault("neighbor_limit", 16L);
            readRequest.neighborLimit = neighborLimit.intValue();
            readRequest.options = (String) payload.get("options");
            @SuppressWarnings("unchecked")
            List<String> fields = (List<String>) payload.get("fields");

            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();
            ContextRestReadAuth.authorizeGetRequest(mgr, reader, readRequest);

            ContextQueryService queryService = new ContextQueryService(mgr, reader);
            ContextQueryService.ReadResult result = queryService.read(readRequest);

            GetResponse resp = new GetResponse();
            resp.requestId = ContextRestAuth.currentRequestId();
            resp.level = readRequest.level.name();
            if ("--history".equals(readRequest.options)) {
                resp.rows = decodeHistoryRows(result.historyRows);
                resp.rowCount = resp.rows.size();
                sendResultByJson(request, response, resp);
                return;
            }
            if (result.row == null) {
                sendResult(request, response, new RestBaseResult("entity not found"));
                return;
            }
            Map<String, Object> row = toRowMap(result.row, result.selectedLines, readRequest.options);
            if (fields != null && !fields.isEmpty()) {
                row = filterFields(row, fields);
            } else if (readRequest.level == ContextReadExecutor.DisclosureLevel.PREVIEW) {
                // Spec §16: PREVIEW is a routing-decision payload (~20 tokens) — name + one-line
                // preview, not the document itself. Drop the heavy fields so the response stays
                // within budget; STANDARD / DEEP keep the full row.
                row.remove("body");
                row.remove("raw_markdown");
                row.remove("frontmatter_json");
                row.remove("source");
            }
            resp.rows = new ArrayList<>();
            resp.rows.add(row);
            resp.rowCount = 1;
            resp.neighbourPreviews = decodeNeighbourPreviewRows(result.neighbourPreviews);
            resp.neighbourPreviewCount = resp.neighbourPreviews.size();
            resp.neighbourBodies = decodeNeighbourBodies(result.neighbourBodies, queryService);
            resp.neighbourBodyCount = resp.neighbourBodies.size();
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response, ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (com.starrocks.authorization.AccessDeniedException e) {
            throw e;
        } catch (Exception e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    private Map<String, Object> toRowMap(ContextReadExecutor.VersionRow row, List<String> selectedLines, String options) {
        Map<String, Object> out = new LinkedHashMap<>();
        boolean lineSelection = !Strings.isNullOrEmpty(options) && options.startsWith("-L");
        String selectedBody = lineSelection
                ? (selectedLines.isEmpty() ? "" : String.join("\n", selectedLines))
                : row.body;
        out.put("id", row.entityId);
        out.put("entity_key", row.entityKey);
        out.put("entity_type", row.entityType);
        out.put("title", row.title);
        out.put("preview", row.preview);
        out.put("version", row.version);
        out.put("snapshot_version", row.snapshotVersion);
        out.put("confidence", row.confidence);
        out.put("created_time", row.createdTime);
        out.put("updated_time", row.updatedTime);
        out.put("commit_time", row.commitTime);
        out.put("deleted", row.deleted);
        out.put("source", row.sourceJson);
        out.put("frontmatter_json", row.frontmatterJson);
        out.put("raw_markdown", lineSelection ? selectedBody : row.effectiveRawMarkdown());
        out.put("body", lineSelection ? selectedBody : row.body);
        if (!selectedLines.isEmpty()) {
            out.put("selected_lines", selectedLines);
        } else if (lineSelection) {
            out.put("selected_lines", selectedLines);
        }
        return out;
    }

    private Map<String, Object> filterFields(Map<String, Object> row, List<String> fields) {
        Set<String> keep = fields.stream().collect(Collectors.toSet());
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (keep.contains(entry.getKey())) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return filtered;
    }

    private List<Map<String, Object>> decodeNeighbourPreviewRows(JsonArray rows) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonArray data = rows.get(i).getAsJsonObject().getAsJsonArray("data");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", data.size() > 0 && !data.get(0).isJsonNull() ? data.get(0).getAsLong() : null);
            row.put("entity_key", data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsString() : null);
            row.put("preview", data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsString() : null);
            row.put("snapshot_version", data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsLong() : null);
            out.add(row);
        }
        return out;
    }

    private List<Map<String, Object>> decodeNeighbourBodies(JsonArray rows, ContextQueryService queryService) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonArray data = rows.get(i).getAsJsonObject().getAsJsonArray("data");
            ContextQueryService.ReadRequest nested = new ContextQueryService.ReadRequest();
            nested.id = data.get(0).isJsonNull() ? null : data.get(0).getAsLong();
            nested.version = data.get(1).isJsonNull() ? null : data.get(1).getAsLong();
            nested.level = ContextReadExecutor.DisclosureLevel.STANDARD;
            nested.neighborLimit = 8;
            ContextQueryService.ReadResult nestedResult = queryService.read(nested);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", data.size() > 0 && !data.get(0).isJsonNull() ? data.get(0).getAsLong() : null);
            row.put("version", data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsLong() : null);
            row.put("entity_key", data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsString() : null);
            row.put("title", data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsString() : null);
            row.put("body", data.size() > 4 && !data.get(4).isJsonNull() ? data.get(4).getAsString() : null);
            row.put("snapshot_version", data.size() > 5 && !data.get(5).isJsonNull() ? data.get(5).getAsLong() : null);
            row.put("neighbour_previews", decodeNeighbourPreviewRows(nestedResult.neighbourPreviews));
            out.add(row);
        }
        return out;
    }

    private List<Map<String, Object>> decodeHistoryRows(JsonArray rows) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            JsonArray data = rows.get(i).getAsJsonObject().getAsJsonArray("data");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", data.size() > 0 && !data.get(0).isJsonNull() ? data.get(0).getAsLong() : null);
            row.put("version", data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsLong() : null);
            row.put("snapshot_version", data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsLong() : null);
            row.put("updated_time", data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsString() : null);
            row.put("deleted", data.size() > 4
                    && com.starrocks.context.ContextJsonUtil.parseBool(data.get(4)));
            row.put("preview", data.size() > 5 && !data.get(5).isJsonNull() ? data.get(5).getAsString() : null);
            row.put("confidence", data.size() > 6 && !data.get(6).isJsonNull() ? data.get(6).getAsDouble() : null);
            out.add(row);
        }
        return out;
    }

    private String firstNonEmpty(String first, String second) {
        return !Strings.isNullOrEmpty(first) ? first : second;
    }

    private static final class GetResponse {
        @JsonProperty("request_id")
        public String requestId;
        @JsonProperty("row_count")
        public int rowCount;
        public List<Map<String, Object>> rows;
        public String level;
        @JsonProperty("neighbour_preview_count")
        public int neighbourPreviewCount;
        @JsonProperty("neighbour_previews")
        public List<Map<String, Object>> neighbourPreviews = new ArrayList<>();
        @JsonProperty("neighbour_body_count")
        public int neighbourBodyCount;
        @JsonProperty("neighbour_bodies")
        public List<Map<String, Object>> neighbourBodies = new ArrayList<>();
    }
}
