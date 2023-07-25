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
import com.starrocks.common.UserException;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.thrift.TUniqueId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TransactionStateBatchTest {
    private static String fileName = "./TransactionStateBatchTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerDe() throws IOException, UserException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        Long dbId = 1000L;
        Long tableId = 20000L;
        UUID uuid = UUID.randomUUID();
        List<TransactionState> transactionStateList = new ArrayList<TransactionState>();
        TransactionState transactionState1 = new TransactionState(dbId, Lists.newArrayList(tableId),
                3000, "label1", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);
        uuid = UUID.randomUUID();
        TransactionState transactionState2 = new TransactionState(dbId, Lists.newArrayList(tableId),
                3001, "label2", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);
        transactionStateList.add(transactionState1);
        transactionStateList.add(transactionState2);

        TransactionStateBatch stateBatch = new TransactionStateBatch(transactionStateList);
        stateBatch.setCompactionScore(1, Quantiles.compute(new ArrayList<>()));

        stateBatch.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TransactionStateBatch readTransactionStateBatch = new TransactionStateBatch();
        readTransactionStateBatch = TransactionStateBatch.read(in);

        Assert.assertEquals(1, readTransactionStateBatch.getCompactionScores().size());
        Assert.assertEquals(readTransactionStateBatch.getTableId(), tableId.longValue());
        Assert.assertEquals(2, readTransactionStateBatch.getTxnIds().size());

        TransactionState state = readTransactionStateBatch.index(0);
        Assert.assertEquals(state.getTransactionId(), 3000L);
        Assert.assertEquals(state.getTransactionId(), transactionState1.getTransactionId());
        Assert.assertEquals(state.getDbId(), dbId.longValue());

        readTransactionStateBatch.setTransactionStatus(TransactionStatus.VISIBLE);
        Assert.assertEquals(TransactionStatus.VISIBLE, state.getTransactionStatus());

        in.close();
    }

}