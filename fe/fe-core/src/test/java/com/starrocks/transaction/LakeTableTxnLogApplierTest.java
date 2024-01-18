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

import com.starrocks.cloudnative.LakeTable;
import org.junit.Assert;
import org.junit.Test;

public class LakeTableTxnLogApplierTest extends LakeTableTestHelper {
    @Test
    public void testCommitAndApply() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assert.assertEquals(1, table.getPartition(partitionId).getVisibleVersion());
        Assert.assertEquals(3, table.getPartition(partitionId).getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assert.assertEquals(2, table.getPartition(partitionId).getVisibleVersion());
        Assert.assertEquals(3, table.getPartition(partitionId).getNextVersion());
        Assert.assertEquals(partitionCommitInfo.getVersionTime(), table.getPartition(partitionId).getVisibleVersionTime());
    }

    @Test
    public void testApplyCommitLogWithDroppedPartition() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId - 1, 2, 0);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assert.assertEquals(1, table.getPartition(partitionId).getVisibleVersion());
        Assert.assertEquals(2, table.getPartition(partitionId).getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assert.assertEquals(1, table.getPartition(partitionId).getVisibleVersion());
        Assert.assertEquals(2, table.getPartition(partitionId).getNextVersion());
    }
}
