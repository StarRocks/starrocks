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

import com.starrocks.context.allocator.ContextIdAllocator;
import com.starrocks.qe.SimpleExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Minimal facade for submitting semantic-context background tasks. Task rows are persisted in
 * {@link ContextInternalTables#TASKS} so operators can observe pending work via
 * {@code SHOW CONTEXT TASKS}; the heavy lifting (dequeue, retry, success/failure state transitions)
 * is delegated to the existing {@code com.starrocks.scheduler.TaskManager} by Milestone 3.
 *
 * <p>Task kinds of interest: async embedding generation, markdown → page splitting, derived page
 * regeneration, reference resync, workspace commit merge. All four use the same payload shape: a
 * {@code task_id}, a kind tag, a JSON payload, and a state cursor. Writes go through
 * {@link SimpleExecutor#executeDML(String)}, which also keeps this class testable without booting
 * the full scheduler.
 */
public class ContextTaskScheduler {

    private static final Logger LOG = LogManager.getLogger(ContextTaskScheduler.class);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public enum TaskKind {
        BULK_IMPORT,
        DERIVED_PAGE,
        REFERENCE_RESYNC,
        WORKSPACE_COMMIT
    }

    /**
     * Per-kind wake queue. {@link #submit} offers the new task_id here so the matching
     * {@link ContextTaskDaemon} can wake from its {@code poll(timeout)} instead of waiting for the
     * next periodic tick. The queue is a best-effort signal channel — losing a wake (e.g. on
     * follower FE because the daemon checks {@code isLeader} and returns early, or on producer
     * exception before {@code offer}) only falls back to the periodic scan in
     * {@link ContextTaskDaemon#runAfterCatalogReady}, never wedges a task forever.
     */
    private final Map<TaskKind, BlockingQueue<Long>> wakeQueues = new EnumMap<>(TaskKind.class);

    public ContextTaskScheduler() {
        for (TaskKind kind : TaskKind.values()) {
            wakeQueues.put(kind, new LinkedBlockingQueue<>());
        }
    }

    /**
     * Returns the wake queue for the given task kind. Consumers (the per-kind {@code
     * ContextTaskDaemon} threads) block on this queue with a finite timeout to combine
     * event-driven dispatch with a periodic safety scan.
     */
    public BlockingQueue<Long> wakeQueueFor(TaskKind kind) {
        return wakeQueues.get(kind);
    }

    public long submit(long contextBaseId, TaskKind kind, String payloadJson) {
        long taskId = ContextIdAllocator.next();
        String now = TS_FMT.format(LocalDateTime.now());
        String sql = String.format(
                "INSERT INTO %s.%s (task_id, contextbase_id, task_type, state, payload_json, "
                        + "created_time, updated_time, error_message) VALUES (%d, %d, '%s', 'PENDING', "
                        + "%s, '%s', '%s', NULL)",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS,
                taskId, contextBaseId, kind.name(),
                payloadJson == null ? "NULL" : "PARSE_JSON('" + ContextSqlEscape.body(payloadJson) + "')",
                now, now);
        SimpleExecutor.getRepoExecutor().executeDML(sql);
        // Signal the matching daemon to wake immediately. `offer` is non-blocking — if the queue
        // is bounded or the daemon is gone we just rely on the next periodic scan. We deliberately
        // do not catch and swallow here; LinkedBlockingQueue.offer never throws.
        wakeQueues.get(kind).offer(taskId);
        LOG.info("submitted context task {} kind={} contextbase={}", taskId, kind, contextBaseId);
        return taskId;
    }

    public void markRunning(long taskId) {
        updateState(taskId, "RUNNING", null);
    }

    public void markCompleted(long taskId) {
        updateState(taskId, "COMPLETED", null);
    }

    public void markFailed(long taskId, String errorMessage) {
        updateState(taskId, "FAILED", errorMessage);
    }

    private void updateState(long taskId, String newState, String errorMessage) {
        SimpleExecutor.getRepoExecutor().executeDML(buildUpdateStateSql(taskId, newState, errorMessage));
    }

    /**
     * Returns the DML used by {@code markRunning} / {@code markCompleted} / {@code markFailed}.
     * Extracted so unit tests can pin the structure without booting a BE — the previous
     * implementation issued an INSERT and silently relied on PK replace semantics, which wiped
     * the other columns of the existing row.
     */
    static String buildUpdateStateSql(long taskId, String newState, String errorMessage) {
        String now = TS_FMT.format(LocalDateTime.now());
        // Use UPDATE, not INSERT-replace. The original code reissued an INSERT with sentinel
        // values (contextbase_id=0, task_type='UNKNOWN', payload_json=NULL) and relied on the
        // primary-key engine's replace semantics to overwrite the existing row — which wiped
        // every column except state/error_message. As a result SHOW CONTEXT TASKS showed
        // task_type='UNKNOWN' for every running/completed task, and any sweep retry path that
        // needs payload_json to recover the task had nothing to read.
        if (errorMessage == null) {
            return String.format(
                    "UPDATE %s.%s SET state = '%s', updated_time = '%s', error_message = NULL "
                            + "WHERE task_id = %d",
                    ContextInternalTables.DATABASE, ContextInternalTables.TASKS,
                    newState, now, taskId);
        }
        return String.format(
                "UPDATE %s.%s SET state = '%s', updated_time = '%s', error_message = '%s' "
                        + "WHERE task_id = %d",
                ContextInternalTables.DATABASE, ContextInternalTables.TASKS,
                newState, now, ContextSqlEscape.body(errorMessage), taskId);
    }
}
