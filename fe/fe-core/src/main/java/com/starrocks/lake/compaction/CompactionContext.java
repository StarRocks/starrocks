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

import com.google.common.collect.Lists;
import com.starrocks.proto.CompactResponse;
import com.starrocks.transaction.VisibleStateWaiter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public class CompactionContext {
    private final String partitionName;
    private final long txnId;
    private final long startTs;
    private volatile long commitTs;
    private volatile long finishTs;
    private Map<Long, List<Long>> beToTablets;
    private List<Future<CompactResponse>> responseList;
    private VisibleStateWaiter visibleStateWaiter;

    public CompactionContext(String partitionName, long txnId, long startTs) {
        this.partitionName = partitionName;
        this.txnId = txnId;
        this.startTs = startTs;
        responseList = Lists.newArrayList();
    }

    public long getTxnId() {
        return txnId;
    }

    public void setResponseList(@NotNull List<Future<CompactResponse>> futures) {
        responseList = futures;
    }

    public List<Future<CompactResponse>> getResponseList() {
        return responseList;
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

    public void setBeToTablets(@NotNull Map<Long, List<Long>> beToTablets) {
        this.beToTablets = beToTablets;
    }

    public Map<Long, List<Long>> getBeToTablets() {
        return beToTablets;
    }

    public boolean compactionFinishedOnBE() {
        return responseList.stream().allMatch(Future::isDone);
    }

    public int getNumCompactionTasks() {
        return beToTablets.values().stream().mapToInt(List::size).sum();
    }

    public long getStartTs() {
        return startTs;
    }

    public void setCommitTs(long commitTs) {
        this.commitTs = commitTs;
    }

    public long getCommitTs() {
        return commitTs;
    }

    public void setFinishTs(long finishTs) {
        this.finishTs = finishTs;
    }

    public long getFinishTs() {
        return finishTs;
    }

    public String getFullPartitionName() {
        return partitionName;
    }

    public String getDebugString() {
        return String.format("TxnId=%d partition=%s", txnId, partitionName);
    }
}
