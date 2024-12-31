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

/** A snapshot of {@link TransactionState}. The snapshot can include more members in the future. */
public class TransactionStateSnapshot {

    private final TransactionStatus status;
    private final String reason;

    public TransactionStateSnapshot(TransactionStatus status, String reason) {
        this.status = status;
        this.reason = reason;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "TransactionStateSnapshot{" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                '}';
    }
}
