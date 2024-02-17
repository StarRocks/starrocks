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

package com.starrocks.http.rest.transaction;

import java.util.Arrays;
import java.util.Optional;

public enum TransactionOperation {

    TXN_BEGIN("begin"),

    TXN_PREPARE("prepare"),

    TXN_COMMIT("commit"),

    TXN_ROLLBACK("rollback"),

    TXN_LOAD("load");

    private final String value;

    TransactionOperation(String value) {
        this.value = value;
    }

    /**
     * Parse {@code op} to {@link TransactionOperation}.
     */
    public static Optional<TransactionOperation> parse(String value) {
        return Arrays.stream(values())
                .filter(operation -> operation.getValue().equalsIgnoreCase(value))
                .findFirst();
    }

    /**
     * Check if {@code txnOperation} is the final operation in transaction lifecycle.
     */
    public static boolean isFinalOperation(TransactionOperation txnOperation) {
        return TXN_COMMIT.equals(txnOperation) || TXN_ROLLBACK.equals(txnOperation);
    }

    public String getValue() {
        return value;
    }

}
