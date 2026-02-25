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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class MultiStatementTransactionHandler implements TransactionOperationHandler {

    private static final Logger LOG = LogManager.getLogger(MultiStatementTransactionHandler.class);

    private final TransactionOperationParams txnOperationParams;

    public MultiStatementTransactionHandler(TransactionOperationParams txnOperationParams) {
        this.txnOperationParams = txnOperationParams;
    }

    @Override
    public ResultWrapper handle(BaseRequest request, BaseResponse response) throws StarRocksException {
        TransactionOperation txnOperation = txnOperationParams.getTxnOperation();
        String dbName = txnOperationParams.getDbName();
        String tableName = txnOperationParams.getTableName();
        Long timeoutMillis = txnOperationParams.getTimeoutMillis();
        String label = txnOperationParams.getLabel();
        LOG.info("Handle multi statement stream load transaction label: {} db: {} table: {} op: {}",
                label, dbName, tableName, txnOperation.getValue());

        TransactionResult result = new TransactionResult();
        switch (txnOperation) {
            case TXN_BEGIN:
                String warehouseName = txnOperationParams.getWarehouseName();
                Warehouse warehouse =
                        GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
                ComputeResource computeResource =
                        GlobalStateMgr.getCurrentState().getWarehouseMgr().acquireComputeResource(warehouse.getId());
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().beginMultiStatementLoadTask(
                        dbName, label, "", "", timeoutMillis, result, computeResource);
                return new ResultWrapper(result);
            case TXN_COMMIT:
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().commitLoadTask(
                        label, request.getRequest().headers(), result);
                return new ResultWrapper(result);
            case TXN_ROLLBACK:
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().rollbackLoadTask(label, result);
                return new ResultWrapper(result);
            case TXN_LOAD:
                TNetworkAddress redirectAddr = GlobalStateMgr.getCurrentState()
                        .getStreamLoadMgr().executeLoadTask(
                                label, 0, request.getRequest().headers(), result, dbName, tableName);
                if (!result.stateOK() || result.containMsg()) {
                    return new ResultWrapper(result);
                }
                return new ResultWrapper(redirectAddr);
            default:
                throw new StarRocksException("Unsupported operation: " + txnOperation);
        }
    }
}
