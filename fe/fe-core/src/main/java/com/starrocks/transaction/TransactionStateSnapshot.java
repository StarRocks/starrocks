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
    // Source type of the originating transaction, or null when unknown (e.g. an UNKNOWN status, or a
    // terminal-state cache record loaded from an image that predates source-type persistence). Callers
    // that gate on the source type (see BypassWriteTransactionHandler) must treat null as "not this
    // source" so an untyped outcome cannot satisfy a request that requires a specific source type.
    private final TransactionState.LoadJobSourceType sourceType;

    public TransactionStateSnapshot(TransactionStatus status, String reason) {
        this(status, reason, null);
    }

    public TransactionStateSnapshot(TransactionStatus status, String reason,
                                    TransactionState.LoadJobSourceType sourceType) {
        this.status = status;
        this.reason = reason;
        this.sourceType = sourceType;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public TransactionState.LoadJobSourceType getSourceType() {
        return sourceType;
    }

    @Override
    public String toString() {
        return "TransactionStateSnapshot{" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                ", sourceType=" + sourceType +
                '}';
    }
}