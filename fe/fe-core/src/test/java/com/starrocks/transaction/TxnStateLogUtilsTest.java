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
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * These messages are the only trace left when a transaction drops the rows it wrote, so the marker and
 * the transaction identity they carry are a contract an operator greps for, not incidental wording.
 */
public class TxnStateLogUtilsTest {

    private static TransactionState txnState(long txnId, String label) {
        TransactionState txnState = Mockito.mock(TransactionState.class);
        Mockito.when(txnState.getTransactionId()).thenReturn(txnId);
        Mockito.when(txnState.getLabel()).thenReturn(label);
        return txnState;
    }

    @Test
    public void testIgnoredTabletsMessageCarriesMarkerAndTxnIdentity() {
        String message = TxnStateLogUtils.buildIgnoredTabletsMessage(
                txnState(1001L, "load_label_1"), 20L, 3050L, Lists.newArrayList(11L, 12L));

        Assertions.assertTrue(message.startsWith(TxnStateLogUtils.ROWS_DROPPED_MARKER), message);
        Assertions.assertTrue(message.contains("txn 1001"), message);
        Assertions.assertTrue(message.contains("load_label_1"), message);
        Assertions.assertTrue(message.contains("table 20"), message);
        Assertions.assertTrue(message.contains("physical partition 3050"), message);
        Assertions.assertTrue(message.contains("2 tablets"), message);
        Assertions.assertTrue(message.contains("[11, 12]"), message);
        Assertions.assertFalse(message.contains("..."), message);
    }

    @Test
    public void testIgnoredTabletsMessageTruncatesLongTabletList() {
        List<Long> tabletIds = LongStream.range(0, 25).boxed().collect(Collectors.toList());

        String message = TxnStateLogUtils.buildIgnoredTabletsMessage(
                txnState(1002L, "load_label_2"), 20L, 3050L, tabletIds);

        // the count stays exact even though the list itself is cut short
        Assertions.assertTrue(message.contains("25 tablets"), message);
        Assertions.assertTrue(message.endsWith("..."), message);
        Assertions.assertTrue(message.contains("0, 1, 2"), message);
        Assertions.assertFalse(message.contains("24"), message);
    }

    @Test
    public void testDroppedCommittedPartitionMessage() {
        String message = TxnStateLogUtils.buildDroppedCommittedPartitionMessage(
                txnState(1003L, "load_label_3"), 20L, 3050L);

        Assertions.assertTrue(message.startsWith(TxnStateLogUtils.ROWS_DROPPED_MARKER), message);
        Assertions.assertTrue(message.contains("txn 1003"), message);
        Assertions.assertTrue(message.contains("load_label_3"), message);
        Assertions.assertTrue(message.contains("physical partition 3050"), message);
        Assertions.assertTrue(message.contains("rows are lost"), message);
    }

    @Test
    public void testLogIgnoredTabletsIsQuietWithoutIgnoredTablets() {
        Map<Long, List<Long>> empty = Maps.newHashMap();
        Assertions.assertDoesNotThrow(() -> TxnStateLogUtils.logIgnoredTablets(txnState(1004L, "l"), 20L, empty));

        Map<Long, List<Long>> ignored = Maps.newHashMap();
        ignored.put(3050L, Lists.newArrayList(11L));
        Assertions.assertDoesNotThrow(() -> TxnStateLogUtils.logIgnoredTablets(txnState(1004L, "l"), 20L, ignored));
        Assertions.assertDoesNotThrow(
                () -> TxnStateLogUtils.logDroppedCommittedPartition(txnState(1004L, "l"), 20L, 3050L));
    }
}
