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
import com.starrocks.common.Pair;
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
import java.util.Optional;
import java.util.Set;
import javax.validation.constraints.NotNull;

public class CommitRateLimiter {
    private static final Logger LOG = LogManager.getLogger(CommitRateLimiter.class);

    private final CompactionMgr compactionMgr;
    private final TransactionState transactionState;
    private final long tableId;

    /**
     * Creates a CommitRateLimiter object.
     *
     * @param compactionMgr    The CompactionMgr object used for compacting the lake table. Must not be null.
     * @param transactionState The TransactionState object representing the current transaction state. Must not be null.
     * @param tableId          The id of the table to which the commit rate limiter is applied.
     *
     */
    public CommitRateLimiter(@NotNull CompactionMgr compactionMgr, @NotNull TransactionState transactionState, long tableId) {
        this.compactionMgr = Objects.requireNonNull(compactionMgr, "compactionMgr is null");
        this.transactionState = Objects.requireNonNull(transactionState, "transactionState is null");
        this.tableId = tableId;
    }

    // Minimum absolute time allowed to commit the transaction
    private static long getAllowCommitTime(TransactionState txnState, double compactionScore) {
        Preconditions.checkState(txnState.getWriteDurationMs() >= 0);
        return txnState.getWriteEndTimeMs() +
                delayTimeMs(txnState.getWriteDurationMs(), compactionScore, slowdownThreshold(), slowdownRatio());
    }

    // How many milliseconds to delay before committing
    private static long delayTimeMs(long writeDuration, double compactionScore, double slowdownThreshold,
                                    double slowdownRatio) {
        if (compactionScore <= slowdownThreshold) {
            return 0;
        }
        return (long) (writeDuration * (compactionScore - slowdownThreshold) * slowdownRatio);
    }

    private static double slowdownThreshold() {
        return Math.max(Config.lake_ingest_slowdown_threshold, Config.lake_compaction_score_selector_min_score);
    }

    private static double slowdownRatio() {
        return Config.lake_ingest_slowdown_ratio;
    }

    // 0 means no limit
    static long compactionScoreUpperBound() {
        long upper = Config.lake_compaction_score_upper_bound;
        return upper <= 0 ? 0 : (long) Math.max(upper, Config.lake_compaction_score_selector_min_score);
    }

    /**
     * Checks the commit rate for the given partition IDs and throws a CommitRateExceededException if necessary.
     *
     * @param partitionIds the set of partition IDs to check. Must not be null.
     * @throws CommitFailedException       if the allow commit time exceeds the transaction timeout
     * @throws CommitRateExceededException if the commit rate exceeds the threshold
     */
    public void check(@NotNull Set<Long> partitionIds, long currentTimeMs)
            throws CommitRateExceededException, CommitFailedException {
        Preconditions.checkNotNull(partitionIds, "partitionIds is null");
        // Does not limit the commit rate of compaction transactions
        if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
            return;
        }

        updateWriteDuration(transactionState);
        setAllowCommitTimeOnce(partitionIds);

        long txnId = transactionState.getTransactionId();
        long abortTime = transactionState.getPrepareTime() + transactionState.getTimeoutMs();

        if (transactionState.getAllowCommitTimeMs() >= abortTime) {
            throw new CommitFailedException("Txn " + txnId + " timed out due to ingestion slowdown", txnId);
        }
        if (transactionState.getAllowCommitTimeMs() > currentTimeMs) {
            LOG.info("delay commit of txn {} for {}ms, write took {}ms", transactionState.getTransactionId(),
                    transactionState.getAllowCommitTimeMs() - currentTimeMs,
                    transactionState.getWriteDurationMs());
            // it will show in `show proc '/transactions/xxx/running'`
            transactionState.setReason("Partition's compaction score is larger than " + slowdownThreshold() +
                    ", delay commit for " + (transactionState.getAllowCommitTimeMs() - currentTimeMs) + "ms." +
                    " You can try to increase compaction concurrency.");
            throw new CommitRateExceededException(txnId, transactionState.getAllowCommitTimeMs());
        }
        long upperBound = compactionScoreUpperBound();
        if (upperBound > 0) {
            Optional<Pair<Long, Double>> partitionAndScore = anyCompactionScoreExceedsUpperBound(partitionIds, upperBound);
            if (partitionAndScore.isPresent()) {
                Pair<Long, Double> pair = partitionAndScore.get();
                throw new CommitFailedException("Failed to load data into partition " + pair.first
                        + ", because of too large compaction score, current/limit: " + pair.second
                        + "/" + upperBound + ". You can reduce the loading job concurrency, " 
                        + "or increase compaction concurrency", txnId);
            }
        }
    }

    private void updateWriteDuration(@NotNull TransactionState txnState) {
        if (txnState.getWriteDurationMs() < 0) {
            txnState.setWriteDurationMs(Math.max(txnState.getWriteEndTimeMs() - txnState.getPrepareTime(), 0));
        }
    }

    private void setAllowCommitTimeOnce(@NotNull Set<Long> partitionIds) {
        if (transactionState.getAllowCommitTimeMs() < 0) {
            double maxCompactionScore = 0;
            for (Long partitionId : partitionIds) {
                maxCompactionScore = Math.max(maxCompactionScore, getPartitionCompactionScore(partitionId));
            }
            long allowCommitTime = getAllowCommitTime(transactionState, maxCompactionScore);
            transactionState.setAllowCommitTimeMs(allowCommitTime);
        }
    }

    private double getPartitionCompactionScore(@NotNull Long partitionId) {
        long dbId = transactionState.getDbId();
        // TODO: Should be able to fetch statistics by partition id
        PartitionIdentifier partitionIdentifier = new PartitionIdentifier(dbId, tableId, partitionId);
        PartitionStatistics statistics = compactionMgr.getStatistics(partitionIdentifier);
        Quantiles compactionScore = statistics != null ? statistics.getCompactionScore() : null;
        return compactionScore != null ? compactionScore.getMax() : 0;
    }

    // Returns the first partition id and its compaction score that exceeds the upper bound
    private Optional<Pair<Long, Double>> anyCompactionScoreExceedsUpperBound(@NotNull Set<Long> partitionIds, long upperBound) {
        for (Long partitionId : partitionIds) {
            double compactionScore = getPartitionCompactionScore(partitionId);
            if (compactionScore > upperBound) {
                return Optional.of(new Pair<>(partitionId, compactionScore));
            }
        }
        return Optional.empty();
    }
}
