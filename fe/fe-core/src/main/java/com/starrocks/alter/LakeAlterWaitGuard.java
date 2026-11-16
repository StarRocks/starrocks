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

package com.starrocks.alter;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.CommitRateLimiter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Cancels a lake alter job that waits for earlier transactions once commits into its table are rejected.
 *
 * <p>A lake table is in SCHEMA_CHANGE or ROLLUP for the whole of such a job, and the compaction scheduler does not
 * compact it meanwhile (CompactionScheduler#startCompaction). Loads keep raising the compaction score while the job
 * waits, and once a partition's score passes lake_compaction_score_upper_bound, new load commits into that partition
 * are rejected (CommitRateLimiter). If a PREPARED transaction writes to that partition, it can keep the job waiting and
 * the partition unavailable for writes until it is rolled back or times out. Cancelling the waiting job lets
 * compaction resume once the table returns to NORMAL.
 */
public final class LakeAlterWaitGuard {
    private static final Logger LOG = LogManager.getLogger(LakeAlterWaitGuard.class);

    private LakeAlterWaitGuard() {
    }

    /**
     * Throws to cancel a job when it is still waiting for a transaction and a partition of its table rejects commits.
     * Any partition of the table counts, not only those the waited-for transactions write: a transaction's partitions
     * are known only once it commits, and while a partition rejects commits, letting the job go on would keep the
     * whole table from compacting until the job finishes. The current partitions, their scores and the oldest
     * transaction the job waits for are read under the same table read lock; publishing takes the table write lock
     * while it makes transactions visible and updates the compaction scores.
     */
    public static void cancelIfCommitsRejected(AlterJobV2 job, long watershedTxnId) throws AlterCancelException {
        if (!Config.lake_enable_ingest_slowdown || Config.lake_compaction_score_upper_bound <= 0) {
            return;
        }
        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        long dbId = job.getDbId();
        long tableId = job.getTableId();
        TransactionState waitedFor;
        CommitRateLimiter.RejectingPartition partition;
        try (AutoCloseableLock ignored = new AutoCloseableLock(dbId, tableId, LockType.READ)) {
            if (!(stateMgr.getLocalMetastore().getTable(dbId, tableId) instanceof OlapTable table)) {
                return;
            }
            if (table.getState() != OlapTable.OlapTableState.SCHEMA_CHANGE
                    && table.getState() != OlapTable.OlapTableState.ROLLUP) {
                return;
            }
            Optional<CommitRateLimiter.RejectingPartition> rejecting =
                    CommitRateLimiter.findPartitionRejectingCommits(stateMgr.getCompactionMgr(), dbId, table);
            if (rejecting.isEmpty()) {
                return;
            }
            waitedFor = stateMgr.getGlobalTransactionMgr().getOldestPreviousRunningTransaction(
                    watershedTxnId, dbId, List.of(tableId));
            if (waitedFor == null) {
                return;
            }
            partition = rejecting.get();
        }
        String message = String.format(Locale.ROOT, "Cancelled while waiting for transaction %d (label %s, %s) to "
                        + "finish: commits into partition %d are "
                        + "rejected because its compaction score %s is above lake_compaction_score_upper_bound %d, "
                        + "and the table does not compact while this job runs. Retry the ALTER after the transaction "
                        + "has finished and the partition's compaction score is back under the bound.",
                waitedFor.getTransactionId(), waitedFor.getLabel(), waitedFor.getTransactionStatus(),
                partition.partitionId(), partition.compactionScore(), partition.upperBound());
        LOG.warn("{} job {} on table {}: {}", job.getType(), job.getJobId(), tableId, message);
        throw new AlterCancelException(message);
    }

    /**
     * Runs the check on a round in which the job gets no compute resource and so cannot reach its waiting branch.
     * A job that has recorded its watershed waits for the transactions below it, in WAITING_TXN or still in PENDING:
     * an index fast-path job journals its watershed while PENDING and enters WAITING_TXN only in memory, so after a
     * leader change it comes back in PENDING with the watershed recorded.
     */
    static void checkWithoutComputeResource(AlterJobV2 job) {
        AlterJobV2.JobState state = job.getJobState();
        if (state != AlterJobV2.JobState.WAITING_TXN && state != AlterJobV2.JobState.PENDING) {
            return;
        }
        Optional<Long> watershedTxnId = job.getTransactionId();
        if (watershedTxnId.isEmpty()) {
            return;
        }
        try {
            cancelIfCommitsRejected(job, watershedTxnId.get());
        } catch (AlterCancelException e) {
            job.cancelInternal(e.getMessage());
        }
    }
}
