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


package com.starrocks.load.loadv2;

import com.starrocks.common.StarRocksException;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;

/**
 * Callback interface for insert load transaction which is used for some special table, eg: Materialized View with IVM refresh.
 */
public interface InsertLoadTxnCallback {
    /**
     * Before transaction is committed, do some preparation work.
     * @param txnState TransactionState
     * @throws TransactionException if any error happens
     */
    void beforeCommitted(TransactionState txnState) throws TransactionException;

    /**
     * After transaction is committed, do some follow-up work.
     */
    void afterCommitted(TransactionState txnState, boolean txnOperated) throws StarRocksException;
}
