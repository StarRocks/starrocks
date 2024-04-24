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

import com.starrocks.proto.TxnInfoPB;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public class PartitionPublishVersionData {
    private final long tableId;
    private final long partitionId;
    private final List<TransactionState> transactionStates = new ArrayList<>();
    private final List<Long> commitVersions = new ArrayList<>();
    private final List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
    private final List<TxnInfoPB> txnInfos = new ArrayList<>();

    public PartitionPublishVersionData(long tableId, long partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public void addTransaction(@NotNull TransactionState txnState) {
        TableCommitInfo tableCommitInfo = requireNonNull(txnState.getTableCommitInfo(tableId));
        PartitionCommitInfo partitionCommitInfo = requireNonNull(tableCommitInfo.getPartitionCommitInfo(partitionId));
        commitVersions.add(partitionCommitInfo.getVersion());
        partitionCommitInfos.add(partitionCommitInfo);
        transactionStates.add(txnState);
        txnInfos.add(TxnInfoHelper.fromTransactionState(txnState));
    }

    @NotNull
    public List<TransactionState> getTransactionStates() {
        return transactionStates;
    }

    @NotNull
    public List<PartitionCommitInfo> getPartitionCommitInfos() {
        return partitionCommitInfos;
    }

    @NotNull
    public List<Long> getCommitVersions() {
        return commitVersions;
    }

    @NotNull
    public List<TxnInfoPB> getTxnInfos() {
        return txnInfos;
    }
}
