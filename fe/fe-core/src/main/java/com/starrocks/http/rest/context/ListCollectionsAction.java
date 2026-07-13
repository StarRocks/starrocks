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
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextMgr;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code GET /api/collections[?contextbase=<name>]}. Returns collections filtered by optional contextbase.
 */
public class ListCollectionsAction extends RestBaseAction {

    public ListCollectionsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/collections", new ListCollectionsAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, com.starrocks.authorization.AccessDeniedException {
        try {
            String contextBase = request.getSingleParameter("contextbase");
            String collectionTypeFilter = request.getSingleParameter("collection_type");
            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            com.starrocks.context.ContextReadExecutor reader =
                    GlobalStateMgr.getCurrentState().getContextReadExecutor();
            // When a contextbase is named, gate the call on USAGE so unauthorized callers see
            // 401 not silent emptiness. Without one, fall through to per-base filtering so the
            // response only contains collections from bases the caller can read.
            if (contextBase != null && !contextBase.isEmpty()) {
                ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                        ContextRestAuth.BaseAction.USAGE);
            }
            List<ContextMgr.CollectionMeta> visible = ContextRestAuth.filterVisibleCollections(
                    ConnectContext.get(), mgr.listCollections(contextBase));
            List<Entry> entries = new ArrayList<>();
            for (ContextMgr.CollectionMeta m : visible) {
                if (collectionTypeFilter != null && !collectionTypeFilter.isEmpty()
                        && !collectionTypeFilter.equalsIgnoreCase(m.getCollectionType())) {
                    continue;
                }
                Entry entry = new Entry();
                entry.id = m.getId();
                entry.contextbaseId = m.getContextBaseId();
                entry.name = m.getName();
                entry.collectionType = m.getCollectionType();
                entry.entityCount = reader.countEntitiesForCollection(m.getId());
                entry.updatedTime = reader.maxUpdatedTimeForCollection(m.getId());
                entry.status = entry.updatedTime == null ? "EMPTY" : "ACTIVE";
                entry.properties = m.getProperties();
                entries.add(entry);
            }
            Result result = new Result();
            result.requestId = ContextRestAuth.currentRequestId();
            result.collections = entries;
            sendResultByJson(request, response, result);
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }

    private static final class Result {
        @JsonProperty("request_id")
        public String requestId;
        public List<Entry> collections;
    }

    private static final class Entry {
        public long id;

        @JsonProperty("contextbase_id")
        public long contextbaseId;

        public String name;

        @JsonProperty("collection_type")
        public String collectionType;

        @JsonProperty("entity_count")
        public long entityCount;

        @JsonProperty("updated_time")
        public String updatedTime;

        public String status;

        public Map<String, String> properties;
    }
}
