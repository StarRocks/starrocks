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

<<<<<<< HEAD
=======
import com.starrocks.thrift.TTransactionStatus;

import java.util.Arrays;

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
public enum TransactionStatus {
    UNKNOWN(0),
    PREPARE(1),
    COMMITTED(2),
    VISIBLE(3),
    ABORTED(4),
    PREPARED(5);

    private final int flag;

<<<<<<< HEAD
    private TransactionStatus(int flag) {
        this.flag = flag;
    }

    public int value() {
        return flag;
    }

    public static TransactionStatus valueOf(int flag) {
        switch (flag) {
            case 0:
                return UNKNOWN;
            case 1:
                return PREPARE;
            case 2:
                return COMMITTED;
            case 3:
                return VISIBLE;
            case 4:
                return ABORTED;
            case 5:
                return PREPARED;
            default:
                return UNKNOWN;
        }
    }

=======
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

    public boolean isFailed() {
        return this == UNKNOWN || this == ABORTED;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public boolean isFinalStatus() {
        return this == TransactionStatus.VISIBLE || this == TransactionStatus.ABORTED;
    }

    @Override
    public String toString() {
<<<<<<< HEAD
        switch (this) {
            case UNKNOWN:
                return "UNKNOWN";
            case PREPARE:
                return "PREPARE";
            case COMMITTED:
                return "COMMITTED";
            case VISIBLE:
                return "VISIBLE";
            case ABORTED:
                return "ABORTED";
            case PREPARED:
                return "PREPARED";
            default:
                return "UNKNOWN";
        }
=======
        return this.name();
    }

    public int getFlag() {
        return flag;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
