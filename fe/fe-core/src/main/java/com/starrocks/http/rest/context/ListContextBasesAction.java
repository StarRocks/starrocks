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
 * {@code GET /api/contextbases}. Returns the full list of contextbases as JSON.
 */
public class ListContextBasesAction extends RestBaseAction {

    private static final class Result {
        public List<Entry> contextbases;
    }

    private static final class Entry {
        public long id;
        public String name;

        @JsonProperty("collection_count")
        public int collectionCount;

        @JsonProperty("entity_count")
        public long entityCount;

        @JsonProperty("updated_time")
        public String updatedTime;

        public String status;
        public Map<String, String> properties;
    }

    public ListContextBasesAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/contextbases", new ListContextBasesAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        try {
            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            com.starrocks.context.ContextReadExecutor reader =
                    GlobalStateMgr.getCurrentState().getContextReadExecutor();
            // Filter to bases the caller has USAGE on (or admin override). Without this gate the
            // endpoint would enumerate every contextbase in the cluster regardless of grants.
            List<ContextMgr.ContextBaseMeta> visible = ContextRestAuth.filterVisibleBases(
                    ConnectContext.get(), mgr.listContextBases());
            // Two GROUP BY queries amortize across all visible bases; the previous code issued
            // 2 reads per base, an N+1 explosion that dominated FE I/O on operator clusters with
            // many contextbases. listCollections is already O(1) via the in-memory map.
            List<Long> baseIds = new ArrayList<>(visible.size());
            for (ContextMgr.ContextBaseMeta m : visible) {
                baseIds.add(m.getId());
            }
            java.util.Map<Long, Long> entityCounts = reader.bulkCountEntitiesForContextBases(baseIds);
            java.util.Map<Long, String> updatedTimes = reader.bulkMaxUpdatedTimeForContextBases(baseIds);
            List<Entry> entries = new ArrayList<>();
            for (ContextMgr.ContextBaseMeta m : visible) {
                Entry entry = new Entry();
                entry.id = m.getId();
                entry.name = m.getName();
                entry.collectionCount = mgr.listCollections(m.getName()).size();
                entry.entityCount = entityCounts.getOrDefault(m.getId(), 0L);
                entry.updatedTime = updatedTimes.get(m.getId());
                entry.status = entry.updatedTime == null ? "EMPTY" : "ACTIVE";
                entry.properties = m.getProperties();
                entries.add(entry);
            }
            Result result = new Result();
            result.contextbases = entries;
            sendResultByJson(request, response, result);
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }
}
