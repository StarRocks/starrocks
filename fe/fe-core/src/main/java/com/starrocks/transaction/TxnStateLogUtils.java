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

package com.starrocks.transaction;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Logging for the places where a transaction silently drops the rows it wrote, because the partition
 * holding them disappeared while the transaction was running. This is allowed on purpose, dropping a
 * partition or a rollup during a load must not fail the load, but it also means the transaction reaches
 * VISIBLE and the load reports success with rows missing.
 * <p>
 * These log lines are the only trace such a loss leaves behind, so they share one marker to make them
 * greppable, and they carry the transaction identity needed to correlate them with a load.
 */
public class TxnStateLogUtils {
    private static final Logger LOG = LogManager.getLogger(TxnStateLogUtils.class);

    // Single marker for every place rows can be dropped this way, grep the FE log for it to tell whether
    // a cluster has already lost rows to a concurrent partition replacement.
    public static final String ROWS_DROPPED_MARKER = "TXN_ROWS_DROPPED";

    // Keep the log line bounded for a load that wrote into many tablets.
    private static final int MAX_REPORTED_TABLETS = 10;

    private TxnStateLogUtils() {
    }

    /**
     * Report tablets ignored at commit time because their physical partition no longer exists.
     *
     * @param ignoredTabletsByPartition physical partition id -> ignored tablet ids, empty means nothing to report
     */
    public static void logIgnoredTablets(TransactionState txnState, long tableId,
                                         Map<Long, List<Long>> ignoredTabletsByPartition) {
        for (Map.Entry<Long, List<Long>> entry : ignoredTabletsByPartition.entrySet()) {
            LOG.warn(buildIgnoredTabletsMessage(txnState, tableId, entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Report a committed partition that disappeared before its transaction could be published.
     */
    public static void logDroppedCommittedPartition(TransactionState txnState, long tableId, long physicalPartitionId) {
        LOG.warn(buildDroppedCommittedPartitionMessage(txnState, tableId, physicalPartitionId));
    }

    @VisibleForTesting
    static String buildIgnoredTabletsMessage(TransactionState txnState, long tableId, long physicalPartitionId,
                                             List<Long> tabletIds) {
        String reported = tabletIds.subList(0, Math.min(tabletIds.size(), MAX_REPORTED_TABLETS))
                + (tabletIds.size() > MAX_REPORTED_TABLETS ? " ..." : "");
        return String.format("%s: txn %d (label %s) wrote %d tablets of table %d physical partition %d which no"
                        + " longer exists, those rows are dropped while the transaction still succeeds, tablets: %s",
                ROWS_DROPPED_MARKER, txnState.getTransactionId(), txnState.getLabel(), tabletIds.size(),
                tableId, physicalPartitionId, reported);
    }

    @VisibleForTesting
    static String buildDroppedCommittedPartitionMessage(TransactionState txnState, long tableId,
                                                        long physicalPartitionId) {
        return String.format("%s: txn %d (label %s) committed into table %d physical partition %d which was dropped"
                        + " before publish, the partition is removed from the transaction and its rows are lost",
                ROWS_DROPPED_MARKER, txnState.getTransactionId(), txnState.getLabel(), tableId, physicalPartitionId);
    }
}
