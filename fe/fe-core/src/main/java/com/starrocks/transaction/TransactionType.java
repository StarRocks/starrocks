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

import com.starrocks.proto.TxnTypePB;
import com.starrocks.thrift.TTxnType;

public enum TransactionType {
    TXN_NORMAL(0),
    TXN_REPLICATION(1);

    private final int value;

    TransactionType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public TTxnType toThrift() {
        switch (value) {
            case 0:
                return TTxnType.TXN_NORMAL;

            case 1:
                return TTxnType.TXN_REPLICATION;

            default:
                throw new RuntimeException("Unknown transaction type value: " + value);
        }
    }

    public TxnTypePB toProto() {
        switch (value) {
            case 0:
                return TxnTypePB.TXN_NORMAL;

            case 1:
                return TxnTypePB.TXN_REPLICATION;

            default:
                throw new RuntimeException("Unknown transaction type value: " + value);
        }
    }
}
