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

package com.starrocks.alter.reshard;

import com.starrocks.server.GlobalStateMgr;

import java.util.HashMap;
import java.util.Map;

/**
 * Edge-triggered, per-table latch that suppresses re-issuing a deterministic reshard split on an
 * unchanged table layout, which makes no progress and just churns tablets. Used by
 * {@link ColocateChecker} to stop its self-sustaining alignment-job storm and by
 * {@link TabletReshardJobMgr} to stop the size-based auto-split trigger from re-firing on an
 * un-splittable (single-key) tablet. Each consumer holds its own instance and supplies its own
 * convergence signature; this class only remembers the last attempt per table and decides whether to
 * re-fire.
 *
 * <p>The latch is keyed per TABLE (not per colocate group) because alignment jobs are per-table:
 * keying by group would let one table's attempt suppress a peer that had never been attempted, or miss
 * a peer's abort. An entry is cleared when the table becomes aligned ({@link #forgetTable}), and the
 * whole map is cleared when no group is unstable ({@link #clear}); it also naturally re-arms when the
 * table's convergence signature changes, on a bounded number of aborts, or after an FE restart (the
 * map is in-memory only).
 *
 * <p>This is a plain collaborator held by {@link ColocateChecker}, mirroring
 * {@link ColocateConvergenceCache}; it owns no thread and is only touched from the checker's single
 * scheduler tick, so it needs no synchronization.
 */
class TableAlignmentLatch {
    // Cap on consecutive re-fires after an aborted alignment attempt on an unchanged layout, so a
    // persistently-aborting job cannot become its own (slow) storm. Past the cap the table is treated
    // like a deterministic dead-end and left unaligned until its layout/data changes.
    static final int ALIGNMENT_ABORT_RETRY_CAP = 3;

    private final Map<Long, TableAlignmentAttempt> lastAttemptByTable = new HashMap<>();

    /**
     * What the checker last did for one table: the {@link ColocateChecker#tableConvergenceSignature}
     * observed when it fired, the alignment job id submitted, and how many times an aborted attempt has
     * been re-fired on this same signature. {@code suppressionLogged} keeps the "no progress" warning to
     * once per stuck state.
     */
    static final class TableAlignmentAttempt {
        final long signature;
        final long jobId;
        final int abortRetries;
        boolean suppressionLogged;

        TableAlignmentAttempt(long signature, long jobId, int abortRetries) {
            this.signature = signature;
            this.jobId = jobId;
            this.abortRetries = abortRetries;
        }
    }

    /** Outcome of {@link #decideAlignment}: whether to fire this cycle and the abort counter to store. */
    record AlignmentDecision(boolean fire, int nextAbortRetries) {
    }

    /**
     * Pure decision function: should the checker (re)issue an alignment job for a table this cycle, and
     * if it fires what abort-retry count should it record? Fire when this is the first attempt
     * ({@code prev == null}) or the layout/data changed since the last attempt (real progress is
     * possible); otherwise the last completed attempt made no progress on this exact input and — because
     * split/merge is deterministic — a re-issue would too, so suppress. The sole exception is a previous
     * attempt that ended in an abort (a transient failure, not a deterministic dead-end): re-fire, but
     * only up to {@link #ALIGNMENT_ABORT_RETRY_CAP} times on an unchanged signature. {@code
     * nextAbortRetries} is only meaningful when {@code fire} is true and a job is actually submitted.
     */
    static AlignmentDecision decideAlignment(TableAlignmentAttempt prev, long currentSignature,
            boolean prevJobAborted) {
        if (prev == null || prev.signature != currentSignature) {
            return new AlignmentDecision(true, 0);
        }
        boolean fire = prevJobAborted && prev.abortRetries < ALIGNMENT_ABORT_RETRY_CAP;
        return new AlignmentDecision(fire, prev.abortRetries + 1);
    }

    /**
     * Whether to (re)issue an alignment job for {@code tableId} at {@code signature} this cycle. Wraps
     * {@link #decideAlignment} with the per-table attempt lookup and the aborted-job probe.
     */
    AlignmentDecision evaluate(long tableId, long signature) {
        TableAlignmentAttempt prev = lastAttemptByTable.get(tableId);
        return decideAlignment(prev, signature, prev != null && isJobAborted(prev.jobId));
    }

    /**
     * When a table is currently suppressed (a settled attempt made no progress on an unchanged layout),
     * returns {@code true} exactly once per stuck state so the caller logs the "no progress" warning once.
     */
    boolean claimSuppressionLog(long tableId) {
        TableAlignmentAttempt prev = lastAttemptByTable.get(tableId);
        if (prev != null && !prev.suppressionLogged && isJobSettled(prev.jobId)) {
            prev.suppressionLogged = true;
            return true;
        }
        return false;
    }

    /** Records that an alignment job was fired for {@code tableId} so an unchanged next cycle latches. */
    void recordFired(long tableId, long signature, long jobId, int nextAbortRetries) {
        lastAttemptByTable.put(tableId, new TableAlignmentAttempt(signature, jobId, nextAbortRetries));
    }

    /** Drops the table's latch entry so a future misalignment re-arms (the table became aligned). */
    void forgetTable(long tableId) {
        lastAttemptByTable.remove(tableId);
    }

    /** Drops all latch state (no colocate group is unstable, so there is nothing to converge). */
    void clear() {
        lastAttemptByTable.clear();
    }

    /** Visible for testing: whether a table currently has a recorded alignment attempt (is latched). */
    boolean hasRecordedAttempt(long tableId) {
        return lastAttemptByTable.containsKey(tableId);
    }

    /** The tracked reshard job for {@code jobId}, or {@code null} if it is no longer tracked. */
    private static TabletReshardJob trackedJob(long jobId) {
        return GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJob(jobId);
    }

    /** True iff the alignment job is still tracked and ended in {@code ABORTED}. */
    private static boolean isJobAborted(long jobId) {
        TabletReshardJob job = trackedJob(jobId);
        return job != null && job.isAborted();
    }

    /** True iff the alignment job has reached a terminal state (or is no longer tracked). */
    private static boolean isJobSettled(long jobId) {
        TabletReshardJob job = trackedJob(jobId);
        return job == null || job.isDone();
    }
}
