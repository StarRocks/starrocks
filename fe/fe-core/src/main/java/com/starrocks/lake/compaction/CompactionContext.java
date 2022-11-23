// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    private long txnId;
    private long startTs;
    private long commitTs;
    private long visibleTs;
    private String partitionName;
    private Map<Long, List<Long>> beToTablets;
    private List<Future<CompactResponse>> responseList;
    private VisibleStateWaiter visibleStateWaiter;

    public CompactionContext() {
        responseList = Lists.newArrayList();
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
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

    public void setStartTs(long startTs) {
        this.startTs = startTs;
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

    public void setVisibleTs(long visibleTs) {
        this.visibleTs = visibleTs;
    }

    public long getVisibleTs() {
        return visibleTs;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getDebugString() {
        return String.format("TxnId=%d partition=%s", txnId, partitionName);
    }
}
