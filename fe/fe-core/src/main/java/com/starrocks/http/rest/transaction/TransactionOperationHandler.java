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

import com.starrocks.common.StarRocksException;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionStateSnapshot;

/**
 * Transaction management request handler.
 */
public interface TransactionOperationHandler {

    /**
     * Handle transaction management request.
     */
    ResultWrapper handle(BaseRequest request, BaseResponse response) throws StarRocksException;

    /**
     * Echo the cached transaction id into an eviction-recovery response so its shape matches the live
     * path, which always returns TxnId alongside Label. A terminal-state cache record always carries a
     * real id (the id is the cache's key and is persisted in the image), so the guard omits the field only
     * for an id-less snapshot -- the truly-UNKNOWN outcomes built via the shorter constructors, which the
     * eviction-recovery success branches never reach. The {@code > 0} test (rather than
     * {@code != NO_TXN_ID}) also suppresses a GSON-defaulted 0 from any record whose image JSON lacked the
     * id, so a bogus {@code "TxnId": 0} can never be emitted either.
     */
    static void addCachedTxnId(TransactionResult result, TransactionStateSnapshot snapshot) {
        long txnId = snapshot.getTxnId();
        if (txnId > 0) {
            result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
        }
    }

    class ResultWrapper {

        private final TransactionResult result;

        private final TNetworkAddress redirectAddress;

        public ResultWrapper(TransactionResult result) {
            this(result, null);
        }

        public ResultWrapper(TNetworkAddress redirectAddress) {
            this(null, redirectAddress);
        }

        public ResultWrapper(TransactionResult result, TNetworkAddress redirectAddress) {
            this.result = result;
            this.redirectAddress = redirectAddress;
        }

        public TransactionResult getResult() {
            return result;
        }

        public TNetworkAddress getRedirectAddress() {
            return redirectAddress;
        }
    }

}
