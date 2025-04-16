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

package com.starrocks.lake.compaction;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.proto.CompactStat;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.VisibleStateWaiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CompactionJob {
    private static final Logger LOG = LogManager.getLogger(CompactionJob.class);
    private final Database db;
    private final Table table;
    private final PhysicalPartition partition;
    private final long txnId;
    private final long startTs;
    private volatile long commitTs;
    private volatile long finishTs;
    private VisibleStateWaiter visibleStateWaiter;
    private List<CompactionTask> tasks = Collections.emptyList();
    private boolean allowPartialSuccess = false;

    public CompactionJob(Database db, Table table, PhysicalPartition partition, long txnId,
            boolean allowPartialSuccess) {
        this.db = Objects.requireNonNull(db, "db is null");
        this.table = Objects.requireNonNull(table, "table is null");
        this.partition = Objects.requireNonNull(partition, "partition is null");
        this.txnId = txnId;
        this.startTs = System.currentTimeMillis();
        this.commitTs = 0L;
        this.finishTs = 0L;
        this.allowPartialSuccess = allowPartialSuccess;
    }

    Database getDb() {
        return db;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTasks(List<CompactionTask> tasks) {
        this.tasks = Objects.requireNonNull(tasks, "tasks is null");
    }

    public String getFailMessage() {
        CompactionTask task = tasks.stream().filter(t ->
                t.getResult() != CompactionTask.TaskResult.ALL_SUCCESS).findAny().orElse(null);
        return task != null ? task.getFailMessage() : null;
    }

    public void setVisibleStateWaiter(VisibleStateWaiter visibleStateWaiter) {
        this.visibleStateWaiter = visibleStateWaiter;
    }

    public boolean waitTransactionVisible(long timeout, TimeUnit unit) {
        return visibleStateWaiter.await(timeout, unit);
    }

    public boolean transactionHasCommitted() {
        return visibleStateWaiter != null;
    }

    public List<TabletCommitInfo> buildTabletCommitInfo() {
        return tasks.stream().map(CompactionTask::buildTabletCommitInfo).flatMap(List::stream).collect(Collectors.toList());
    }

    public CompactionTask.TaskResult getResult() {
        int allSuccess = 0;
        int partialSuccess = 0;
        int noneSuccess = 0;
        for (CompactionTask task : tasks) {
            CompactionTask.TaskResult subTaskResult = task.getResult();
            switch (subTaskResult) {
                case NOT_FINISHED:
                    return subTaskResult; // early return
                case PARTIAL_SUCCESS:
                    partialSuccess++;
                    break;
                case NONE_SUCCESS:
                    noneSuccess++;
                    break;
                case ALL_SUCCESS:
                    allSuccess++;
                    break;
                default:
                    Preconditions.checkArgument(false, "unhandled compaction task result: %s", subTaskResult.name());
                    break;
            }
        }
        if (allSuccess == tasks.size()) {
            return CompactionTask.TaskResult.ALL_SUCCESS;
        } else if (noneSuccess == tasks.size()) {
            return CompactionTask.TaskResult.NONE_SUCCESS;
        } else {
            return CompactionTask.TaskResult.PARTIAL_SUCCESS;
        }
    }

    public int getNumTabletCompactionTasks() {
        return tasks.stream().filter(Predicate.not(CompactionTask::isDone)).mapToInt(CompactionTask::tabletCount).sum();
    }

    public long getStartTs() {
        return startTs;
    }

    public long getCommitTs() {
        return commitTs;
    }

    public void setCommitTs(long commitTs) {
        this.commitTs = commitTs;
    }

    public long getFinishTs() {
        return finishTs;
    }

    public void finish() {
        this.finishTs = System.currentTimeMillis();
    }

    public void abort() {
        tasks.forEach(CompactionTask::abort);
    }

    public PhysicalPartition getPartition() {
        return partition;
    }

    public String getFullPartitionName() {
        return String.format("%s.%s.%s", db.getFullName(), table.getName(), partition.getId());
    }

    public String getDebugString() {
        return String.format("TxnId=%d partition=%s", txnId, getFullPartitionName());
    }

    public boolean getAllowPartialSuccess() {
        return allowPartialSuccess;
    }

    public String getExecutionProfile() {
        if (tasks.isEmpty() || finishTs == 0L) {
            return "";
        }
        CompactStat stat = new CompactStat();
        stat.subTaskCount = 0;
        stat.readTimeRemote = 0L;
        stat.readBytesRemote = 0L;
        stat.readTimeLocal = 0L;
        stat.readBytesLocal = 0L;
        stat.inQueueTimeSec = 0;
        for (CompactionTask task : tasks) {
            List<CompactStat> subStats = task.getCompactStats();
            if (subStats == null) {
                continue;
            }
            int subTaskCount = 0;
            for (CompactStat subStat : subStats) {
                if (subStat.subTaskCount != null) {
                    subTaskCount = subStat.subTaskCount;
                }
                if (subStat.readTimeRemote != null) {
                    stat.readTimeRemote += subStat.readTimeRemote;
                }
                if (subStat.readBytesRemote != null) {
                    stat.readBytesRemote += subStat.readBytesRemote;
                }
                if (subStat.readTimeLocal != null) {
                    stat.readTimeLocal += subStat.readTimeLocal;
                }
                if (subStat.readBytesLocal != null) {
                    stat.readBytesLocal += subStat.readBytesLocal;
                }
                if (subStat.inQueueTimeSec != null) {
                    stat.inQueueTimeSec += subStat.inQueueTimeSec;
                }
            }
            stat.subTaskCount += subTaskCount;
        }
        return new CompactionProfile(stat).toString();
    }
}
