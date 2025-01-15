// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.starrocks.proto.TransactionStatusPB;
import com.starrocks.thrift.TTransactionStatus;

import java.util.Arrays;

public enum TransactionStatus {
    UNKNOWN(0),
    PREPARE(1),
    COMMITTED(2),
    VISIBLE(3),
    ABORTED(4),
    PREPARED(5);

    private final int flag;

    TransactionStatus(int flag) {
        this.flag = flag;
    }

    public static TransactionStatus valueOf(int flag) {
        return Arrays.stream(values())
                .filter(status -> status.getFlag() == flag)
                .findFirst()
                .orElse(UNKNOWN);
    }

    public TTransactionStatus toThrift() {
        switch (this.getFlag()) {
            // UNKNOWN
            case 0:
                return TTransactionStatus.UNKNOWN;
            // PREPARE
            case 1:
                return TTransactionStatus.PREPARE;
            // COMMITTED
            case 2:
                return TTransactionStatus.COMMITTED;
            // VISIBLE
            case 3:
                return TTransactionStatus.VISIBLE;
            // ABORTED
            case 4:
                return TTransactionStatus.ABORTED;
            // PREPARED
            case 5:
                return TTransactionStatus.PREPARED;
            default:
                return TTransactionStatus.UNKNOWN;
        }
    }

    public TransactionStatusPB toProto() {
        switch (this.getFlag()) {
            // UNKNOWN
            case 0:
                return TransactionStatusPB.TRANS_UNKNOWN;
            // PREPARE
            case 1:
                return TransactionStatusPB.TRANS_PREPARE;
            // COMMITTED
            case 2:
                return TransactionStatusPB.TRANS_COMMITTED;
            // VISIBLE
            case 3:
                return TransactionStatusPB.TRANS_VISIBLE;
            // ABORTED
            case 4:
                return TransactionStatusPB.TRANS_ABORTED;
            // PREPARED
            case 5:
                return TransactionStatusPB.TRANS_PREPARED;
            default:
                return TransactionStatusPB.TRANS_UNKNOWN;
        }
    }

    public boolean isFailed() {
        return this == UNKNOWN || this == ABORTED;
    }

    public boolean isFinalStatus() {
        return this == TransactionStatus.VISIBLE || this == TransactionStatus.ABORTED;
    }

    @Override
    public String toString() {
        return this.name();
    }

    public int getFlag() {
        return flag;
    }
}
