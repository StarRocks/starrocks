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

import com.google.common.collect.Lists;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.TabletStatPB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionCommitInfoTest {
    private static final long PID = 12345L;
    private static final long TABLE_ID = 6789L;

    // Round-trip a PartitionCommitInfo through TableCommitInfo + TransactionState copies, the path
    // used by finish/replay, and return the copied PartitionCommitInfo.
    private PartitionCommitInfo copyViaTransactionState(PartitionCommitInfo pci) {
        TableCommitInfo tableCommitInfo = new TableCommitInfo(TABLE_ID);
        tableCommitInfo.addPartitionCommitInfo(pci);
        TransactionState state = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 0, 60_000);
        state.putIdToTableCommitInfo(TABLE_ID, tableCommitInfo);

        TransactionState copied = new TransactionState(state);
        return copied.getTableCommitInfo(TABLE_ID).getPartitionCommitInfo(PID);
    }

    @Test
    public void testTabletStatsCopiedByCopyConstructor() {
        PartitionCommitInfo src = new PartitionCommitInfo(1L, 2L, 3L);
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 10L;
        stat.dataSize = 100L;
        src.getTabletStats().put(7L, stat);

        PartitionCommitInfo copy = new PartitionCommitInfo(src);
        Assertions.assertEquals(1, copy.getTabletStats().size());
        Assertions.assertEquals(100L, copy.getTabletStats().get(7L).dataSize);
    }

    @Test
    public void testPartitionCommitInfoCopyAndJsonRoundTrip() {
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, 5, 0);

        // copy ctor preserves version
        Assertions.assertEquals(5L, new PartitionCommitInfo(pci).getVersion());

        // GSON round-trip preserves version
        Assertions.assertEquals(5L,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pci), PartitionCommitInfo.class).getVersion());

        // TableCommitInfo copy
        TableCommitInfo tableCommitInfo = new TableCommitInfo(TABLE_ID);
        tableCommitInfo.addPartitionCommitInfo(pci);
        Assertions.assertEquals(5L,
                new TableCommitInfo(tableCommitInfo).getPartitionCommitInfo(PID).getVersion());

        // through TransactionState copy used by finish/replay
        Assertions.assertEquals(5L, copyViaTransactionState(pci).getVersion());
    }

    @Test
    public void testShadowRewriteTxnUsesSourceType() {
        // Shadow-rewrite partitions use sentinel version -1; the guard is now on
        // TransactionState.isShadowRewrite() (sourceType == SHADOW_REWRITE), not on a
        // per-partition boolean.
        PartitionCommitInfo pci = new PartitionCommitInfo(PID, -1, 0);

        // PartitionCommitInfo itself has no isShadowRewrite marker; verify it round-trips normally.
        Assertions.assertEquals(-1L, new PartitionCommitInfo(pci).getVersion());
        Assertions.assertEquals(-1L,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pci), PartitionCommitInfo.class).getVersion());

        // The txn-level isShadowRewrite is driven by sourceType.
        TransactionState txn = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.SHADOW_REWRITE, null, 0, 60_000);
        Assertions.assertTrue(txn.isShadowRewrite());

        TransactionState normal = new TransactionState(1000L, Lists.newArrayList(TABLE_ID),
                3000, "label", null, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 0, 60_000);
        Assertions.assertFalse(normal.isShadowRewrite());
    }
}
