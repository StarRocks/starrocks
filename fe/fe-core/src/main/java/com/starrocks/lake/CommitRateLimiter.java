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

package com.starrocks.lake;

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.PartitionStatistics;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.TransactionState;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Set;
import javax.validation.constraints.NotNull;

public class CommitRateLimiter {
    private static final Logger LOG = LogManager.getLogger(CommitRateLimiter.class);

    private final CompactionMgr compactionMgr;
    private final LakeTable table;
    private final TransactionState transactionState;

    /**
     * Creates a CommitRateLimiter object.
     *
     * @param compactionMgr    The CompactionMgr object used for compacting the lake table. Must not be null.
     * @param table            The LakeTable object to which the commit rate limiter is applied. Must not be null.
     * @param transactionState The TransactionState object representing the current transaction state. Must not be null.
     */
    public CommitRateLimiter(@NotNull CompactionMgr compactionMgr,
                             @NotNull LakeTable table,
                             @NotNull TransactionState transactionState) {
        this.compactionMgr = Objects.requireNonNull(compactionMgr, "compactionMgr is null");
        this.table = Objects.requireNonNull(table, "table is null");
        this.transactionState = Objects.requireNonNull(transactionState, "transactionState is null");
    }

    private static long getCommitTime(TransactionState transactionState, double maxCompactionScore, double threshold) {
        double slowDownPercentage = (maxCompactionScore - threshold) * Config.lake_delay_commit_compaction_score_ratio;
        // The time spending on data writing
        long writeDurationMs = transactionState.getDoneWriteTime() - transactionState.getPrepareTime();
        // The bigger "writeDurationMs" is, the bigger "slowDownTime" is
        long slowDownTime = (long) (writeDurationMs * slowDownPercentage);
        return transactionState.getDoneWriteTime() + slowDownTime;
    }

    /**
     * Checks the commit rate for the given partition IDs and throws a CommitRateExceededException if necessary.
     *
     * @param partitionIds the set of partition IDs to check. Must not be null.
     * @throws CommitFailedException if the allow commit time exceeds the transaction timeout
     * @throws CommitRateExceededException if the commit rate exceeds the threshold
     */
    public void check(@NotNull Set<Long> partitionIds) throws CommitRateExceededException, CommitFailedException {
        Preconditions.checkNotNull(partitionIds, "partitionIds is null");
        // Does not limit the commit rate of compaction transactions
        if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
            return;
        }
        long txnId = transactionState.getTransactionId();
        long currentMs = System.currentTimeMillis();
        if (transactionState.getAllowCommitTime() < 0) {
            long dbId = transactionState.getDbId();
            long tableId = table.getId();
            long maxWaitForTime = 0;
            for (Long partitionId : partitionIds) {
                PartitionIdentifier partitionIdentifier = new PartitionIdentifier(dbId, tableId, partitionId);
                maxWaitForTime = Math.max(maxWaitForTime, commitWaitForTime(partitionIdentifier, currentMs));
            }
            transactionState.setAllowCommitTime(currentMs + maxWaitForTime);
            if (maxWaitForTime > 0) {
                LOG.info("delay commit of txn {} for {}ms, write took {}ms", txnId, maxWaitForTime,
                        transactionState.getDoneWriteTime() - transactionState.getPrepareTime());
            }
        }
        long abortTime = transactionState.getPrepareTime() + transactionState.getTimeoutMs();
        if (transactionState.getAllowCommitTime() >= abortTime) {
            throw new CommitFailedException("Txn " + txnId + " timed out due to commit throttling", txnId);
        }
        if (transactionState.getAllowCommitTime() > currentMs) {
            throw new CommitRateExceededException(txnId, transactionState.getAllowCommitTime());
        }
    }

    private long commitWaitForTime(@NotNull PartitionIdentifier partitionIdentifier, long currentMilliseconds) {
        PartitionStatistics statistics = compactionMgr.getStatistics(partitionIdentifier);
        Quantiles compactionScore = statistics != null ? statistics.getCompactionScore() : null;
        if (compactionScore == null) {
            return 0;
        }
        double maxCompactionScore = compactionScore.getMax();
        double threshold = Math.max(Config.lake_delay_commit_compaction_score_threshold,
                Config.lake_compaction_score_selector_min_score);
        if (maxCompactionScore <= threshold) {
            return 0;
        }
        long commitTime = getCommitTime(transactionState, maxCompactionScore, threshold);
        long commitWaitMs = commitTime - currentMilliseconds;
        if (commitWaitMs < 0) {
            return 0;
        } else {
            return commitWaitMs;
        }
    }
}
