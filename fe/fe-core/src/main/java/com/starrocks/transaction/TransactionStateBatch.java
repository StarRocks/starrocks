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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TransactionStateBatch implements Writable {

    private static final Logger LOG = LogManager.getLogger(TransactionStateBatch.class);

    @SerializedName("transactionStates")
    List<TransactionState> transactionStates = new ArrayList<>();

    // partitionId -> beId -> tabletId
    // used to clean txnLog when publish succeeded,
    // no need to persist, just try the best effort,
    // for vacuum will clean the txnLog finally
    ConcurrentHashMap<Long, Map<ComputeNode, Set<Long>>> partitionToTablets = new ConcurrentHashMap<>();

    public TransactionStateBatch() {
    }

    public TransactionStateBatch(List<TransactionState> transactionStates) {
        this.transactionStates = transactionStates;
    }

    // No concurrency issues.
    // Because in the case of concurrent calls,
    // the partitionId will not be the same,
    // and transactionStates is read-only which will not be changed
    public void setCompactionScore(long tableId, long partitionId, Quantiles quantiles) {
        this.transactionStates.stream()
                .map(transactionState -> transactionState.getTableCommitInfo(tableId))
                .filter(commitInfo -> commitInfo.getPartitionCommitInfo(partitionId) != null)
                .forEach(commitInfo -> commitInfo.getPartitionCommitInfo(partitionId).setCompactionScore(quantiles));
    }

    public void putBeTablets(long partitionId, Map<ComputeNode, List<Long>> nodeToTablets)  {
        for (Map.Entry<ComputeNode, List<Long>> nodeTablets : nodeToTablets.entrySet()) {
            Map<ComputeNode, Set<Long>> oneNodeTablets =
                    partitionToTablets.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>());
            Set<Long> tablets = oneNodeTablets.computeIfAbsent(nodeTablets.getKey(), k -> ConcurrentHashMap.newKeySet());
            tablets.addAll(nodeTablets.getValue());
        }
    }

    public ConcurrentHashMap<Long, Map<ComputeNode, Set<Long>>> getPartitionToTablets() {
        return partitionToTablets;
    }

    public void setTransactionVisibleInfo() {
        for (TransactionState transactionState : transactionStates) {
            transactionState.setFinishTime(System.currentTimeMillis());
            transactionState.clearErrorMsg();
            transactionState.setNewFinish();
            transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
            transactionState.notifyVisible();
        }
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        for (TransactionState state : transactionStates) {
            state.setTransactionStatus(transactionStatus);
        }
    }

    // a proxy method
    public void afterVisible(TransactionStatus transactionStatus, boolean txnOperated) {
        for (TransactionState transactionState : transactionStates) {
            for (Long callbackId : transactionState.getCallbackId()) {
                // after status changed
                TxnStateChangeCallback callback = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .getCallbackFactory().getCallback(callbackId);
                if (callback != null) {
                    if (Objects.requireNonNull(transactionStatus) == TransactionStatus.VISIBLE) {
                        callback.afterVisible(transactionState, txnOperated);
                    }
                }
            }
        }
    }

    // all transactionState in TransactionStateBatch have the same dbId
    public long getDbId() {
        if (transactionStates.size() != 0) {
            return transactionStates.get(0).getDbId();
        }
        return -1;
    }

    public List<Long> getTxnIds() {
        return transactionStates.stream().map(TransactionState::getTransactionId).collect(Collectors.toList());
    }

    // all transactionState in batch have the same table and return the tableId
    public long getTableId() {
        if (!transactionStates.isEmpty()) {
            List<Long> tableIdList = transactionStates.get(0).getTableIdList();
            assert tableIdList.size() == 1;
            return tableIdList.get(0);
        }
        return -1;
    }

    public long size() {
        return transactionStates.size();
    }

    public TransactionState index(int index) throws StarRocksException {
        if (index < 0 || index >= transactionStates.size()) {
            throw new StarRocksException("index out of bound");
        }
        return transactionStates.get(index);
    }

    public List<TransactionState> getTransactionStates() {
        return transactionStates;
    }

    public void writeLock() {
        for (TransactionState transactionState : transactionStates) {
            transactionState.writeLock();
        }
    }

    public void writeUnlock() {
        for (TransactionState transactionState : transactionStates) {
            transactionState.writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TransactionStateBatch read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TransactionStateBatch.class);
    }

    @Override
    public String toString() {
        return transactionStates.toString();
    }
}