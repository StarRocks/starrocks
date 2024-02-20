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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.VisibleStateWaiter;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CompactionJob {
    private final Database db;
    private final Table table;
    private final Partition partition;
    private final long txnId;
    private final long startTs;
    private volatile long commitTs;
    private volatile long finishTs;
    private VisibleStateWaiter visibleStateWaiter;
    private List<CompactionTask> tasks;

    public CompactionJob(Database db, Table table, Partition partition, long txnId) {
        this.db = Objects.requireNonNull(db, "db is null");
        this.table = Objects.requireNonNull(table, "table is null");
        this.partition = Objects.requireNonNull(partition, "partition is null");
        this.txnId = txnId;
        this.startTs = System.currentTimeMillis();
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

    public boolean isFailed() {
        return tasks.stream().anyMatch(CompactionTask::isFailed);
    }

    public String getFailMessage() {
        CompactionTask task = tasks.stream().filter(CompactionTask::isFailed).findAny().orElse(null);
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

    public boolean isCompleted() {
        return tasks.stream().allMatch(CompactionTask::isCompleted);
    }

    public int getNumTabletCompactionTasks() {
        return tasks.stream().filter(t -> !t.isDone()).mapToInt(CompactionTask::tabletCount).sum();
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

    public Partition getPartition() {
        return partition;
    }

    public String getFullPartitionName() {
        return String.format("%s.%s.%s", db.getFullName(), table.getName(), partition.getName());
    }

    public String getDebugString() {
        return String.format("TxnId=%d partition=%s", txnId, getFullPartitionName());
    }
}
