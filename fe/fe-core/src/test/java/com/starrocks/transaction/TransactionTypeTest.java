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

import com.starrocks.proto.TxnTypePB;
import com.starrocks.thrift.TTxnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TransactionTypeTest {
    @Test
    public void testTransactionType() {
        Assertions.assertEquals(0, TransactionType.TXN_NORMAL.value());
        Assertions.assertEquals(1, TransactionType.TXN_REPLICATION.value());

        Assertions.assertEquals(TTxnType.TXN_NORMAL, TransactionType.TXN_NORMAL.toThrift());
        Assertions.assertEquals(TTxnType.TXN_REPLICATION, TransactionType.TXN_REPLICATION.toThrift());

        Assertions.assertEquals(TxnTypePB.TXN_NORMAL, TransactionType.TXN_NORMAL.toProto());
        Assertions.assertEquals(TxnTypePB.TXN_REPLICATION, TransactionType.TXN_REPLICATION.toProto());
    }
}
