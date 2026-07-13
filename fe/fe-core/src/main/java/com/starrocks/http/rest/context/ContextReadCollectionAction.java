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
import com.starrocks.context.SnapshotResolver;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/read-collection}. Body:
 * {@code {"contextbase": "sales_ai", "collection": "pipeline_rules", "as_of_time": "2026-03-01",
 *   "limit": 500, "offset": 0, "after_entity_id": 12345}}.
 * When {@code as_of_time} is absent, returns current head rows; otherwise walks the versions table
 * through the snapshot fence resolver.
 *
 * <p>Pagination contract:
 * <ul>
 *   <li>Rows are ordered by {@code entity_id ASC} on every page (unique, monotonic — stable under
 *       concurrent upserts and tie-break-deterministic in MPP).</li>
 *   <li>{@code after_entity_id} (keyset cursor) is the recommended pagination primitive — O(log N + N)
 *       per page, stable under writes. Caller resumes by passing the previous response's
 *       {@code next_after_entity_id} back in.</li>
 *   <li>{@code offset} still works (the previous "ignored" behavior was a bug), but cost is
 *       O(offset + limit) per call. Prefer cursor on collections with more than a few pages.</li>
 *   <li>When both are supplied, {@code after_entity_id} wins.</li>
 *   <li>{@code next_after_entity_id} is the {@code entity_id} of the last row returned, or
 *       {@code null} when the page returned fewer than {@code limit} rows (i.e., end-of-scan).</li>
 * </ul>
 */
public class ContextReadCollectionAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextReadCollectionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/read-collection",
                new ContextReadCollectionAction(controller));
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
            String contextBase = (String) payload.get("contextbase");
            String collection = (String) payload.get("collection");
            if (Strings.isNullOrEmpty(contextBase) || Strings.isNullOrEmpty(collection)) {
                sendResult(request, response,
                        new RestBaseResult("\"contextbase\" and \"collection\" are required"));
                return;
            }
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                    ContextRestAuth.BaseAction.USAGE);

            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextMgr.ContextBaseMeta cb = mgr.getContextBase(contextBase);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult("contextbase not found: " + contextBase));
                return;
            }
            ContextMgr.CollectionMeta col = null;
            for (ContextMgr.CollectionMeta m : mgr.listCollections(contextBase)) {
                if (collection.equals(m.getName())) {
                    col = m;
                    break;
                }
            }
            if (col == null) {
                sendResult(request, response, new RestBaseResult("collection not found: " + collection));
                return;
            }

            long snapshotFence = -1L;
            String asOf = (String) payload.get("as_of_time");
            if (!Strings.isNullOrEmpty(asOf)) {
                SnapshotResolver resolver = GlobalStateMgr.getCurrentState().getContextSnapshotResolver();
                snapshotFence = resolver.resolveFromSelector(cb.getId(), asOf);
                if (snapshotFence < 0) {
                    sendResult(request, response, new RestBaseResult(
                            "no snapshot visible at as_of_time=" + asOf));
                    return;
                }
            }
            Number limit = (Number) payload.getOrDefault("limit", 500L);
            int effectiveLimit = limit.intValue();
            Number offset = (Number) payload.getOrDefault("offset", 0L);
            int effectiveOffset = Math.max(0, offset.intValue());
            Number afterEntityId = (Number) payload.getOrDefault("after_entity_id", -1L);
            long effectiveAfter = afterEntityId.longValue();
            JsonArray rows = GlobalStateMgr.getCurrentState().getContextReadExecutor()
                    .readCollection(col.getId(), snapshotFence, effectiveLimit,
                            effectiveOffset, effectiveAfter);

            ReadResponse resp = new ReadResponse();
            resp.snapshotFence = snapshotFence;
            resp.rowCount = rows.size();
            resp.rows = decodeRows(rows);
            resp.nextAfterEntityId = computeNextAfterEntityId(resp.rows, effectiveLimit);
            sendResultByJson(request, response, resp);
        } catch (JsonSyntaxException e) {
            sendResult(request, response, new RestBaseResult("invalid JSON body"));
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }

    /**
     * Materialize the SELECT result into named-field rows. The current-head and as-of paths now
     * share the same full-version schema as {@link com.starrocks.context.ContextReadExecutor.VersionRow}:
     * {@code 0:id, 1:version, 2:entity_key, 3:entity_type, 4:contextbase_id, 5:collection_id,
     * 6:title, 7:preview, 8:body, 9:raw_markdown, 10:frontmatter_json, 11:source_json,
     * 12:confidence, 13:created_time, 14:updated_time, 15:commit_time, 16:snapshot_version,
     * 17:deleted}.
     */
    private List<Map<String, Object>> decodeRows(JsonArray rows) {
        List<Map<String, Object>> out = new ArrayList<>(rows.size());
        for (com.google.gson.JsonElement el : rows) {
            JsonArray data = el.getAsJsonObject().getAsJsonArray("data");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", data.size() > 0 && !data.get(0).isJsonNull() ? data.get(0).getAsLong() : null);
            row.put("version", data.size() > 1 && !data.get(1).isJsonNull() ? data.get(1).getAsLong() : null);
            row.put("entity_key", data.size() > 2 && !data.get(2).isJsonNull() ? data.get(2).getAsString() : null);
            row.put("entity_type", data.size() > 3 && !data.get(3).isJsonNull() ? data.get(3).getAsString() : null);
            row.put("contextbase_id",
                    data.size() > 4 && !data.get(4).isJsonNull() ? data.get(4).getAsLong() : null);
            row.put("collection_id",
                    data.size() > 5 && !data.get(5).isJsonNull() ? data.get(5).getAsLong() : null);
            row.put("title", data.size() > 6 && !data.get(6).isJsonNull() ? data.get(6).getAsString() : null);
            row.put("preview", data.size() > 7 && !data.get(7).isJsonNull() ? data.get(7).getAsString() : null);
            row.put("body", data.size() > 8 && !data.get(8).isJsonNull() ? data.get(8).getAsString() : null);
            row.put("raw_markdown",
                    data.size() > 9 && !data.get(9).isJsonNull() ? data.get(9).getAsString() : null);
            // frontmatter_json / source_json are JSON-typed columns; need toString() for non-primitives.
            row.put("frontmatter_json",
                    data.size() > 10 && !data.get(10).isJsonNull() ? jsonElementToRawString(data.get(10)) : null);
            row.put("source", data.size() > 11 && !data.get(11).isJsonNull() ? jsonElementToRawString(data.get(11)) : null);
            row.put("confidence",
                    data.size() > 12 && !data.get(12).isJsonNull() ? data.get(12).getAsDouble() : null);
            row.put("created_time",
                    data.size() > 13 && !data.get(13).isJsonNull() ? data.get(13).getAsString() : null);
            row.put("updated_time",
                    data.size() > 14 && !data.get(14).isJsonNull() ? data.get(14).getAsString() : null);
            row.put("commit_time",
                    data.size() > 15 && !data.get(15).isJsonNull() ? data.get(15).getAsString() : null);
            row.put("snapshot_version",
                    data.size() > 16 && !data.get(16).isJsonNull() ? data.get(16).getAsLong() : null);
            row.put("deleted", data.size() > 17
                    && com.starrocks.context.ContextJsonUtil.parseBool(data.get(17)));
            out.add(row);
        }
        return out;
    }

    private static String jsonElementToRawString(com.google.gson.JsonElement el) {
        return el.isJsonPrimitive() ? el.getAsString() : el.toString();
    }

    /**
     * Cursor for the next page. {@code null} signals end-of-scan (fewer than {@code limit} rows
     * returned, so a subsequent call would yield 0 rows anyway). Callers loop on
     * {@code next_after_entity_id != null}.
     */
    static Long computeNextAfterEntityId(List<Map<String, Object>> rows, int limit) {
        if (rows.isEmpty() || rows.size() < limit) {
            return null;
        }
        Object lastId = rows.get(rows.size() - 1).get("id");
        return lastId instanceof Number ? ((Number) lastId).longValue() : null;
    }

    private static final class ReadResponse {
        @JsonProperty("snapshot_version")
        public long snapshotFence;

        @JsonProperty("row_count")
        public int rowCount;

        public List<Map<String, Object>> rows;

        @JsonProperty("next_after_entity_id")
        public Long nextAfterEntityId;
    }
}
