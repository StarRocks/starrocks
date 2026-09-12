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

    /**
     * Sentinel {@link #getTxnId()} value meaning "this snapshot carries no transaction id" -- the outcome
     * is UNKNOWN (the label was never seen) or otherwise has no originating transaction, so the snapshot
     * was built via a constructor that does not take an id. A terminal-state cache record always carries a
     * real id (it is the cache's key, and it is persisted in the image), so a cache-sourced snapshot never
     * uses this sentinel. Callers that echo the id back to a client (see the eviction-recovery branches in
     * the HTTP transaction handlers) must skip the field when it is this value so they never emit a bogus
     * {@code "TxnId": -1}.
     */
    public static final long NO_TXN_ID = -1L;

    private final TransactionStatus status;
    private final String reason;
    // Source type of the originating transaction, or null when unknown (e.g. an UNKNOWN status, or a
    // terminal-state cache record loaded from an image that predates source-type persistence). Callers
    // that gate on the source type (see BypassWriteTransactionHandler) must treat null as "not this
    // source" so an untyped outcome cannot satisfy a request that requires a specific source type.
    private final TransactionState.LoadJobSourceType sourceType;
    // Transaction id of the originating transaction, or NO_TXN_ID when there is none (an UNKNOWN outcome,
    // or any snapshot built via a constructor that omits the id). Carried so an eviction-recovery response
    // can echo TxnId exactly like the live path does, keeping the HTTP response shape identical across
    // count eviction; a terminal cache record always supplies a real id here.
    private final long txnId;

    public TransactionStateSnapshot(TransactionStatus status, String reason) {
        this(status, reason, null, NO_TXN_ID);
    }

    public TransactionStateSnapshot(TransactionStatus status, String reason,
                                    TransactionState.LoadJobSourceType sourceType) {
        this(status, reason, sourceType, NO_TXN_ID);
    }

    public TransactionStateSnapshot(TransactionStatus status, String reason,
                                    TransactionState.LoadJobSourceType sourceType, long txnId) {
        this.status = status;
        this.reason = reason;
        this.sourceType = sourceType;
        this.txnId = txnId;
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

    public long getTxnId() {
        return txnId;
    }

    @Override
    public String toString() {
        return "TransactionStateSnapshot{" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                ", sourceType=" + sourceType +
                ", txnId=" + txnId +
                '}';
    }
}