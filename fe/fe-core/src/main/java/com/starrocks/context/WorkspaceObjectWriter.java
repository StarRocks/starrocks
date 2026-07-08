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

package com.starrocks.context;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.starrocks.context.allocator.ContextSnapshotAllocator;
import com.starrocks.qe.SimpleExecutor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Writes workspace objects to {@link ContextInternalTables#WORKSPACE_OBJECTS}. Objects are versioned
 * per {@code (workspace_id, object_id)} pair; TTL is derived from the workspace's own configuration
 * (or overridden per-call via the {@code ttl_hours} option).
 */
public class WorkspaceObjectWriter {

    public static final String WORKSPACE_SCOPE_MEMORY = "memory";
    public static final String WORKSPACE_SCOPE_SCRATCH = "scratch";
    public static final String WORKSPACE_SCOPE_OUTPUT = "output";

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = new Gson();

    private final ContextMgr contextMgr;
    private final ContextSnapshotAllocator snapshotAllocator;

    public WorkspaceObjectWriter(ContextMgr contextMgr, ContextSnapshotAllocator snapshotAllocator) {
        this.contextMgr = contextMgr;
        this.snapshotAllocator = snapshotAllocator;
    }

    public static final class UpsertResult {
        public final long workspaceId;
        public final String objectId;
        public final long version;
        public final long snapshotVersion;

        UpsertResult(long workspaceId, String objectId, long version, long snapshotVersion) {
            this.workspaceId = workspaceId;
            this.objectId = objectId;
            this.version = version;
            this.snapshotVersion = snapshotVersion;
        }
    }

    public UpsertResult upsert(String workspaceName, String objectId, String objectType,
                               Map<String, Object> payload, double priority, long ttlHours) {
        return upsert(workspaceName, objectId, objectType, payload, priority, ttlHours,
                WORKSPACE_SCOPE_SCRATCH);
    }

    public UpsertResult upsert(String workspaceName, String objectId, String objectType,
                               Map<String, Object> payload, double priority, long ttlHours,
                               String workspaceScope) {
        if (Strings.isNullOrEmpty(workspaceName)) {
            throw new IllegalArgumentException("workspace name is required");
        }
        if (Strings.isNullOrEmpty(objectId)) {
            throw new IllegalArgumentException("object_id is required");
        }
        ContextMgr.WorkspaceMeta workspace = requireWorkspace(workspaceName);
        String normalizedScope = normalizeWorkspaceScopeForWrite(workspaceScope);

        long snapshotVersion = snapshotAllocator.next();
        long version = snapshotVersion; // workspace objects share the snapshot namespace for simplicity
        Map<String, Object> payloadOrEmpty = payload == null ? new HashMap<>() : payload;
        String payloadJson = GSON.toJson(payloadOrEmpty);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime expire = now.plusHours(ttlHours <= 0 ? 24 : ttlHours);
        String expireStr = TS_FMT.format(expire);
        String nowStr = TS_FMT.format(now);

        // payload_json is a JSON column written through PARSE_JSON('<sql-literal>'). Gson emits
        // backslash-escaped sequences (\\, \", \n, control chars). StarRocks' SQL string-literal
        // parser collapses one layer of escapes (\\ -> \, \n -> newline), so we MUST escape the
        // Gson output as a SQL body before embedding it; otherwise a single backslash in the
        // payload either mangles the JSON or terminates the literal early. The earlier inline
        // replace("'", "''") only handled single quotes and let everything else through. Route
        // through ContextSqlEscape.body for consistency with every other write path in the
        // module.
        String insert = String.format(
                "INSERT INTO %s.%s (workspace_id, object_id, version, workspace_scope, object_type, payload_json, "
                        + "priority, ttl_expire_time, updated_time, snapshot_version, deleted) "
                        + "VALUES (%d, '%s', %d, %s, %s, PARSE_JSON('%s'), %f, '%s', '%s', %d, false)",
                ContextInternalTables.DATABASE, ContextInternalTables.WORKSPACE_OBJECTS,
                workspace.getId(), escapeSql(objectId), version,
                sqlString(normalizedScope),
                sqlString(Strings.isNullOrEmpty(objectType) ? "draft" : objectType),
                escapeSql(payloadJson),
                priority, expireStr, nowStr, snapshotVersion);
        SimpleExecutor.getRepoExecutor().executeDML(insert);
        return new UpsertResult(workspace.getId(), objectId, version, snapshotVersion);
    }

    public UpsertResult discard(String workspaceName, String objectId) {
        return discard(requireWorkspace(workspaceName), objectId, WORKSPACE_SCOPE_SCRATCH);
    }

    public UpsertResult discard(ContextMgr.WorkspaceMeta workspace, String objectId, String workspaceScope) {
        if (workspace == null || Strings.isNullOrEmpty(objectId)) {
            throw new IllegalArgumentException("workspace and object_id are required");
        }
        long snapshotVersion = snapshotAllocator.next();
        long version = snapshotVersion;
        String scope = normalizeWorkspaceScopeForRead(workspaceScope);
        String now = TS_FMT.format(LocalDateTime.now());
        String insert = String.format(
                "INSERT INTO %s.%s (workspace_id, object_id, version, workspace_scope, object_type, payload_json, "
                        + "priority, ttl_expire_time, updated_time, snapshot_version, deleted) "
                        + "VALUES (%d, '%s', %d, %s, 'tombstone', PARSE_JSON('{}'), 0.0, '%s', '%s', %d, true)",
                ContextInternalTables.DATABASE, ContextInternalTables.WORKSPACE_OBJECTS,
                workspace.getId(), escapeSql(objectId), version, sqlString(scope), now, now, snapshotVersion);
        SimpleExecutor.getRepoExecutor().executeDML(insert);
        return new UpsertResult(workspace.getId(), objectId, version, snapshotVersion);
    }

    public static String normalizeWorkspaceScopeForWrite(String workspaceScope) {
        String normalized = workspaceScope == null ? null : workspaceScope.trim().toLowerCase(Locale.ROOT);
        if (Strings.isNullOrEmpty(normalized)) {
            return WORKSPACE_SCOPE_SCRATCH;
        }
        switch (normalized) {
            case WORKSPACE_SCOPE_MEMORY:
            case WORKSPACE_SCOPE_SCRATCH:
            case WORKSPACE_SCOPE_OUTPUT:
                return normalized;
            default:
                throw new IllegalArgumentException(
                        "workspace_scope must be one of memory, scratch, output");
        }
    }

    public static String normalizeWorkspaceScopeForRead(String workspaceScope) {
        String normalized = workspaceScope == null ? null : workspaceScope.trim().toLowerCase(Locale.ROOT);
        if (Strings.isNullOrEmpty(normalized)) {
            return WORKSPACE_SCOPE_SCRATCH;
        }
        switch (normalized) {
            case WORKSPACE_SCOPE_MEMORY:
            case WORKSPACE_SCOPE_SCRATCH:
            case WORKSPACE_SCOPE_OUTPUT:
                return normalized;
            default:
                return WORKSPACE_SCOPE_SCRATCH;
        }
    }

    private ContextMgr.WorkspaceMeta requireWorkspace(String workspaceName) {
        ContextMgr.WorkspaceMeta workspace = contextMgr.getWorkspace(workspaceName);
        if (workspace == null) {
            throw new IllegalStateException("workspace not found: " + workspaceName);
        }
        return workspace;
    }

    private static String sqlString(String s) {
        return ContextSqlEscape.literal(s);
    }

    private static String escapeSql(String s) {
        return ContextSqlEscape.body(s);
    }
}
