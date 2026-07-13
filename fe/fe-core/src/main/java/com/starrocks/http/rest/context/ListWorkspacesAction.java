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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextInternalTables;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.WorkspaceObjectWriter;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code GET /api/workspaces[?contextbase=<name>]}. Returns workspaces visible to the requester.
 */
public class ListWorkspacesAction extends RestBaseAction {

    public ListWorkspacesAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/workspaces", new ListWorkspacesAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, com.starrocks.authorization.AccessDeniedException {
        try {
            String contextBase = request.getSingleParameter("contextbase");
            ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
            if (contextBase != null && !contextBase.isEmpty()) {
                ContextRestAuth.checkOnContextBase(ConnectContext.get(), contextBase,
                        ContextRestAuth.BaseAction.USAGE);
            }
            List<ContextMgr.WorkspaceMeta> visible = ContextRestAuth.filterVisibleWorkspaces(
                    ConnectContext.get(), mgr.listWorkspaces(contextBase));
            Map<Long, WorkspaceRestSupport.Summary> summaries = WorkspaceRestSupport.summarizeWorkspaces(visible);
            List<Entry> entries = new ArrayList<>();
            for (ContextMgr.WorkspaceMeta m : visible) {
                WorkspaceRestSupport.Summary summary = summaries.get(m.getId());
                Entry entry = new Entry();
                entry.id = m.getId();
                entry.collectionId = m.getCollectionId();
                entry.qualifiedName = m.getName();
                entry.properties = m.getProperties();
                if (summary != null) {
                    entry.memory = summary.memory;
                    entry.scratch = summary.scratch;
                    entry.output = summary.output;
                    entry.lastActivity = summary.lastActivity;
                }
                entries.add(entry);
            }
            Result result = new Result();
            result.workspaces = entries;
            sendResultByJson(request, response, result);
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        }
    }

    private static final class Result {
        public List<Entry> workspaces;
    }

    private static final class Entry {
        public long id;

        @JsonProperty("collection_id")
        public long collectionId;

        @JsonProperty("qualified_name")
        public String qualifiedName;

        public Map<String, String> properties;

        public long memory;

        public long scratch;

        public long output;

        @JsonProperty("last_activity")
        public String lastActivity;
    }
}

final class WorkspaceRestSupport {

    private WorkspaceRestSupport() {
    }

    static Map<Long, Summary> summarizeWorkspaces(List<ContextMgr.WorkspaceMeta> workspaces) {
        Map<Long, Summary> result = new LinkedHashMap<>();
        if (workspaces == null || workspaces.isEmpty()) {
            return result;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT workspace_id, object_id, version, workspace_scope, updated_time, deleted FROM ")
                .append(ContextInternalTables.DATABASE).append('.')
                .append(ContextInternalTables.WORKSPACE_OBJECTS)
                .append(" WHERE workspace_id IN (");
        for (int i = 0; i < workspaces.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            ContextMgr.WorkspaceMeta workspace = workspaces.get(i);
            sql.append(workspace.getId());
            result.put(workspace.getId(), new Summary());
        }
        sql.append(") ORDER BY workspace_id ASC, object_id ASC, version DESC");
        for (LatestWorkspaceObjectRow row : loadLatestRows(sql.toString(), false)) {
            if (row.deleted) {
                continue;
            }
            Summary summary = result.get(row.workspaceId);
            if (summary == null) {
                continue;
            }
            summary.increment(row.workspaceScope);
            summary.observe(row.updatedTime);
        }
        return result;
    }

    static List<WorkspaceObjectRef> listActiveObjects(ContextMgr.WorkspaceMeta workspace) {
        List<WorkspaceObjectRef> active = new ArrayList<>();
        if (workspace == null) {
            return active;
        }
        String sql = String.format(
                "SELECT workspace_id, object_id, version, workspace_scope, updated_time, deleted FROM %s.%s "
                        + "WHERE workspace_id = %d ORDER BY workspace_id ASC, object_id ASC, version DESC",
                ContextInternalTables.DATABASE, ContextInternalTables.WORKSPACE_OBJECTS, workspace.getId());
        for (LatestWorkspaceObjectRow row : loadLatestRows(sql, true)) {
            if (row.deleted) {
                continue;
            }
            active.add(new WorkspaceObjectRef(row.objectId, row.workspaceScope));
        }
        return active;
    }

    private static List<LatestWorkspaceObjectRow> loadLatestRows(String sql, boolean failOnError) {
        JsonArray rows = runQuery(sql, failOnError);
        List<LatestWorkspaceObjectRow> latest = new ArrayList<>();
        long lastWorkspaceId = Long.MIN_VALUE;
        String lastObjectId = null;
        for (JsonElement rowEl : rows) {
            JsonArray data = rowEl.getAsJsonObject().getAsJsonArray("data");
            long workspaceId = data.get(0).getAsLong();
            String objectId = stringValue(data, 1);
            if (objectId == null) {
                continue;
            }
            if (workspaceId == lastWorkspaceId && objectId.equals(lastObjectId)) {
                continue;
            }
            lastWorkspaceId = workspaceId;
            lastObjectId = objectId;
            latest.add(new LatestWorkspaceObjectRow(
                    workspaceId,
                    objectId,
                    WorkspaceObjectWriter.normalizeWorkspaceScopeForRead(stringValue(data, 3)),
                    stringValue(data, 4),
                    booleanValue(data, 5)));
        }
        return latest;
    }

    private static JsonArray runQuery(String sql, boolean failOnError) {
        try {
            List<TResultBatch> batches = SimpleExecutor.getRepoExecutor().executeDQL(sql);
            JsonArray rows = new JsonArray();
            for (TResultBatch batch : batches) {
                if (batch.getRows() == null) {
                    continue;
                }
                for (ByteBuffer buf : batch.getRows()) {
                    ByteBuf copied = Unpooled.copiedBuffer(buf);
                    rows.add(JsonParser.parseString(copied.toString(Charset.defaultCharset())));
                }
            }
            return rows;
        } catch (Exception e) {
            if (failOnError) {
                throw new IllegalStateException("workspace object query failed: " + e.getMessage(), e);
            }
            return new JsonArray();
        }
    }

    private static String stringValue(JsonArray data, int idx) {
        if (data == null || data.size() <= idx || data.get(idx).isJsonNull()) {
            return null;
        }
        return data.get(idx).getAsString();
    }

    private static boolean booleanValue(JsonArray data, int idx) {
        return data != null && data.size() > idx
                && com.starrocks.context.ContextJsonUtil.parseBool(data.get(idx));
    }

    static final class Summary {
        long memory;
        long scratch;
        long output;
        String lastActivity;

        void increment(String workspaceScope) {
            switch (workspaceScope) {
                case WorkspaceObjectWriter.WORKSPACE_SCOPE_MEMORY:
                    memory++;
                    break;
                case WorkspaceObjectWriter.WORKSPACE_SCOPE_OUTPUT:
                    output++;
                    break;
                default:
                    scratch++;
                    break;
            }
        }

        void observe(String updatedTime) {
            if (updatedTime == null) {
                return;
            }
            if (lastActivity == null || updatedTime.compareTo(lastActivity) > 0) {
                lastActivity = updatedTime;
            }
        }
    }

    static final class WorkspaceObjectRef {
        final String objectId;
        final String workspaceScope;

        WorkspaceObjectRef(String objectId, String workspaceScope) {
            this.objectId = objectId;
            this.workspaceScope = workspaceScope;
        }
    }

    private static final class LatestWorkspaceObjectRow {
        final long workspaceId;
        final String objectId;
        final String workspaceScope;
        final String updatedTime;
        final boolean deleted;

        private LatestWorkspaceObjectRow(long workspaceId, String objectId, String workspaceScope,
                                         String updatedTime, boolean deleted) {
            this.workspaceId = workspaceId;
            this.objectId = objectId;
            this.workspaceScope = workspaceScope;
            this.updatedTime = updatedTime;
            this.deleted = deleted;
        }
    }
}
