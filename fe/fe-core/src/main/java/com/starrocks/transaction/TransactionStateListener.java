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

import java.util.List;

// TransactionStateListener will be created by the FE master process before it prepares or commits a transaction.
// Used to check state transitions and save table-specific information in the TransactionState.
public interface TransactionStateListener {
    String getTableName();

    // This method is called before changing the in-memory TransactionState to PREPARED.
    void prePrepared(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
            List<TabletFailInfo> failedTablets) throws TransactionException;

    // This method is called before changing a PREPARED transaction to COMMITTED.
    default void preCommit(TransactionState txnState) throws TransactionException {
    }

    // This method is called after changing the in-memory TransactionState to PREPARED and before writing the edit log.
    void preWriteCommitLog(TransactionState txnState);

    // This method is called by the FE master after changed the TransactionState to ABORTED and *AFTER* released the writer
    // lock of the DatabaseTransactionMgr.
    // It's *unsafe* to access mutable fields of txnState inside this function.
    void postAbort(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
            List<TabletFailInfo> failedTablets);
}
