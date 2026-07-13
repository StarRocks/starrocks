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
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.retrieval.ContextPacker;
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
import java.util.List;
import java.util.Map;

/**
 * {@code POST /api/context/pack}. Body:
 * {@code {"entity_ids": [301, 302], "max_tokens": 4000, "include_citations": true}}.
 *
 * <p>For convenience, callers that only have entity_keys (the user-facing string handle, e.g.
 * the bench's sha256-of-path scheme) may pass {@code "entity_keys": ["abc...","def..."]} instead
 * of {@code entity_ids}. The action resolves keys to ids server-side via a single SELECT before
 * running the contextbase membership check, so the caller doesn't need a separate round-trip
 * per key just to look up its id.
 */
public class ContextPackAction extends RestBaseAction {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    public ContextPackAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/context/pack", new ContextPackAction(controller));
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
            @SuppressWarnings("unchecked")
            List<Number> entityIds = (List<Number>) payload.get("entity_ids");
            @SuppressWarnings("unchecked")
            List<String> entityKeys = (List<String>) payload.get("entity_keys");
            if ((entityIds == null || entityIds.isEmpty())
                    && (entityKeys == null || entityKeys.isEmpty())) {
                sendResult(request, response,
                        new RestBaseResult("\"entity_ids\" or \"entity_keys\" is required"));
                return;
            }
            String contextBase = (String) payload.get("contextbase");
            if (Strings.isNullOrEmpty(contextBase)) {
                sendResult(request, response, new RestBaseResult("\"contextbase\" is required"));
                return;
            }
            ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                    ContextRestAuth.BaseAction.USAGE);

            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            ContextMgr.ContextBaseMeta cb = mgr.getContextBase(contextBase);
            if (cb == null) {
                sendResult(request, response, new RestBaseResult(
                        "contextbase not found: " + contextBase));
                return;
            }
            // Verify each requested entity actually belongs to this contextbase. Without this
            // check, a caller authorized for base A could pack entities owned by base B simply by
            // listing their numeric ids. We reject if any id falls outside the named base.
            List<Long> ids = new ArrayList<>();
            if (entityIds != null) {
                for (Number n : entityIds) {
                    ids.add(n.longValue());
                }
            }
            if (entityKeys != null && !entityKeys.isEmpty()) {
                // Resolve string keys to numeric ids in one SELECT against the heads table,
                // scoped to this contextbase. Keys that don't resolve are silently dropped (the
                // packer skips missing ids anyway, mirroring the entity_ids contract). Keys that
                // resolve to a different contextbase are caught by the membership check below.
                ids.addAll(resolveEntityKeys(cb.getId(), entityKeys));
            }
            if (ids.isEmpty()) {
                sendResult(request, response,
                        new RestBaseResult("no entities resolved from supplied entity_ids/entity_keys"));
                return;
            }
            long mismatched = countMismatchedEntities(cb.getId(), ids);
            if (mismatched < 0) {
                // Heads view unavailable — we cannot prove membership, so refuse the pack rather
                // than letting cross-base ids through during a bootstrap / failure window.
                sendResult(request, response, new RestBaseResult(
                        "cannot verify entity membership against contextbase " + contextBase));
                return;
            }
            if (mismatched > 0) {
                sendResult(request, response, new RestBaseResult(
                        mismatched + " entity id(s) do not belong to contextbase " + contextBase));
                return;
            }

            ContextPacker.Request req = new ContextPacker.Request();
            req.entityIds = ids;
            req.maxTokens = ((Number) payload.getOrDefault("max_tokens", 4000L)).intValue();
            req.includeCitations = Boolean.TRUE.equals(payload.getOrDefault("include_citations", true));

            ContextPacker.Result result = GlobalStateMgr.getCurrentState().getContextPacker().pack(req);

            PackResponse resp = new PackResponse();
            resp.packedText = result.packedText;
            resp.usedTokensEstimate = result.usedTokensEstimate;
            resp.includedEntities = result.includedEntities;
            resp.truncatedEntities = result.truncatedEntities;
            resp.citations = new ArrayList<>();
            for (ContextPacker.Citation citation : result.citations) {
                CitationEntry entry = new CitationEntry();
                entry.id = citation.entityId;
                entry.version = citation.version;
                entry.title = citation.title;
                entry.entityKey = citation.entityKey;
                resp.citations.add(entry);
            }
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
     * Resolve the supplied entity_keys to entity_ids, restricted to the given contextbase. One
     * SQL round-trip; missing keys yield no row and are silently dropped (the packer would
     * skip them anyway). Mirrors the LIKE-escape pattern used elsewhere in this package.
     */
    private List<Long> resolveEntityKeys(long contextBaseId, List<String> entityKeys) {
        StringBuilder inList = new StringBuilder();
        boolean first = true;
        for (String key : entityKeys) {
            if (Strings.isNullOrEmpty(key)) {
                continue;
            }
            if (!first) {
                inList.append(',');
            }
            inList.append('\'').append(key.replace("'", "''")).append('\'');
            first = false;
        }
        List<Long> resolved = new ArrayList<>();
        if (first) {
            return resolved;
        }
        String sql = String.format(
                "SELECT entity_id FROM %s.%s WHERE entity_key IN (%s) AND contextbase_id = %d "
                        + "AND current_deleted = false",
                com.starrocks.context.ContextInternalTables.DATABASE,
                com.starrocks.context.ContextInternalTables.HEADS,
                inList, contextBaseId);
        try {
            com.google.gson.JsonArray rows = com.starrocks.context.ContextSqlSupport.executeDql(sql);
            for (int i = 0; i < rows.size(); i++) {
                com.google.gson.JsonArray data = rows.get(i).getAsJsonObject().getAsJsonArray("data");
                if (data.size() > 0 && !data.get(0).isJsonNull()) {
                    resolved.add(data.get(0).getAsLong());
                }
            }
        } catch (Exception e) {
            // Heads may not exist yet during early bootstrap; fall through and let the membership
            // check decide. Caller sees a friendlier "no entities resolved" message.
        }
        return resolved;
    }

    /**
     * Count how many of {@code ids} live in a contextbase other than {@code expectedContextBaseId}.
     * Returns -1 when the SELECT can't run (heads not yet materialized) so callers can refuse the
     * pack without leaking. Returns 0 when every id either matches the expected base or doesn't
     * exist (the pack will simply skip nonexistent ids).
     */
    private long countMismatchedEntities(long expectedContextBaseId, List<Long> ids) {
        StringBuilder inList = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                inList.append(',');
            }
            inList.append(ids.get(i));
        }
        return GlobalStateMgr.getCurrentState().getContextReadExecutor()
                .countWithFilter(com.starrocks.context.ContextInternalTables.HEADS,
                        "entity_id IN (" + inList + ") AND contextbase_id != " + expectedContextBaseId);
    }

    private static final class PackResponse {
        @JsonProperty("packed_text")
        public String packedText;

        @JsonProperty("used_tokens_estimate")
        public int usedTokensEstimate;

        @JsonProperty("included_entities")
        public List<Long> includedEntities;

        @JsonProperty("truncated_entities")
        public List<Long> truncatedEntities;

        public List<CitationEntry> citations;
    }

    private static final class CitationEntry {
        public long id;
        public long version;
        public String title;

        // The user-facing string handle for the entity. Lets clients map a citation back to the
        // original ingest path / external id without a follow-up /api/context/get round-trip.
        @JsonProperty("entity_key")
        public String entityKey;
    }
}
