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

import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TxnInfoHelperTest {

    @Test
    public void testFromTransactionStateWithCompactionForcePublish() {
        TransactionState state = Mockito.mock(TransactionState.class, Mockito.RETURNS_DEEP_STUBS);

        Mockito.when(state.getTransactionId()).thenReturn(42L);
        Mockito.when(state.isUseCombinedTxnLog()).thenReturn(true);
        Mockito.when(state.getCommitTime()).thenReturn(5000L); // ms
        Mockito.when(state.getSourceType()).thenReturn(TransactionState.LoadJobSourceType.LAKE_COMPACTION);

        CompactionTxnCommitAttachment attachment = Mockito.mock(CompactionTxnCommitAttachment.class);
        Mockito.when(attachment.getForceCommit()).thenReturn(true);
        Mockito.when(state.getTxnCommitAttachment()).thenReturn(attachment);

        // Return a Long for gtid instead of String
        Mockito.when(state.getGlobalTransactionId()).thenReturn(1001L);
        Mockito.when(state.getLoadIds()).thenReturn(Arrays.asList(tid(1L, 2L), tid(3L, 4L)));

        // Allow returning null to avoid depending on a specific proto enum in tests
        Mockito.when(state.getTransactionType().toProto()).thenReturn(null);

        TxnInfoPB info = TxnInfoHelper.fromTransactionState(state);

        assertEquals(42L, info.txnId.longValue());
        assertTrue(info.combinedTxnLog);
        assertEquals(5L, info.commitTime.longValue()); // seconds
        assertTrue(info.forcePublish);
        // Compare gtid as long
        assertEquals(1001L, info.getGtid().longValue());

        assertNotNull(info.loadIds);
        assertEquals(2, info.loadIds.size());
        assertEquals(1L, info.loadIds.get(0).getHi().longValue());
        assertEquals(2L, info.loadIds.get(0).getLo().longValue());
        assertEquals(3L, info.loadIds.get(1).getHi().longValue());
        assertEquals(4L, info.loadIds.get(1).getLo().longValue());
    }

    @Test
    public void testFromTransactionStateWithoutCompaction() {
        TransactionState state = Mockito.mock(TransactionState.class, Mockito.RETURNS_DEEP_STUBS);

        Mockito.when(state.getTransactionId()).thenReturn(7L);
        Mockito.when(state.isUseCombinedTxnLog()).thenReturn(false);
        Mockito.when(state.getCommitTime()).thenReturn(0L);
        Mockito.when(state.getSourceType()).thenReturn(TransactionState.LoadJobSourceType.FRONTEND);
        Mockito.when(state.getTxnCommitAttachment()).thenReturn(null);
        // getGlobalTransactionId() returns primitive long, cannot be null
        Mockito.when(state.getGlobalTransactionId()).thenReturn(0L);
        Mockito.when(state.getLoadIds()).thenReturn(null);
        Mockito.when(state.getTransactionType().toProto()).thenReturn(null);

        TxnInfoPB info = TxnInfoHelper.fromTransactionState(state);

        assertEquals(7L, info.txnId.longValue());
        assertFalse(info.combinedTxnLog);
        assertEquals(0L, info.commitTime.longValue());
        assertFalse(info.forcePublish);
        assertNull(info.loadIds);
    }

    private static TUniqueId tid(long hi, long lo) {
        TUniqueId id = new TUniqueId();
        id.setHi(hi);
        id.setLo(lo);
        return id;
    }
}
