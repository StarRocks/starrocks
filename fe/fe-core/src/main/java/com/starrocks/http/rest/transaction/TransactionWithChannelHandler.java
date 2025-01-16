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

import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.http.rest.transaction.TransactionOperationParams.Channel;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Transaction management request handler with channel info (eg. id, num) in request.
 */
public class TransactionWithChannelHandler implements TransactionOperationHandler {

    private static final Logger LOG = LogManager.getLogger(TransactionWithChannelHandler.class);

    private final TransactionOperationParams txnOperationParams;

    public TransactionWithChannelHandler(TransactionOperationParams txnOperationParams) {
        Validate.isTrue(txnOperationParams.getChannel().notNull(), "channel is null");
        this.txnOperationParams = txnOperationParams;
    }

    @Override
    public ResultWrapper handle(BaseRequest request, BaseResponse response) throws UserException {
        TransactionOperation txnOperation = txnOperationParams.getTxnOperation();
        String dbName = txnOperationParams.getDbName();
        String tableName = txnOperationParams.getTableName();
        Long timeoutMillis = txnOperationParams.getTimeoutMillis();
        String label = txnOperationParams.getLabel();
        Channel channel = txnOperationParams.getChannel();
        LOG.info("Handle transaction with channel info, label: {}", label);

        TransactionResult result = new TransactionResult();
        switch (txnOperation) {
            case TXN_BEGIN:
                if (channel.getId() >= channel.getNum() || channel.getId() < 0) {
                    throw new DdlException(String.format(
                            "Channel ID should be between [0, %d].", (channel.getNum() - 1)));
                }
                String warehouseName = txnOperationParams.getWarehouseName();
                Warehouse warehouse =
                        GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().beginLoadTask(
                        dbName, tableName, label, "", "", timeoutMillis, channel.getNum(), channel.getId(), result,
                        warehouse.getId());
                return new ResultWrapper(result);
            case TXN_PREPARE:
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().prepareLoadTask(
                        label, channel.getId(), request.getRequest().headers(), result);
                if (!result.stateOK() || result.containMsg()) {
                    return new ResultWrapper(result);
                }
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().tryPrepareLoadTaskTxn(label, result);
                return new ResultWrapper(result);
            case TXN_COMMIT:
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().commitLoadTask(label, result);
                return new ResultWrapper(result);
            case TXN_ROLLBACK:
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().rollbackLoadTask(label, result);
                return new ResultWrapper(result);
            case TXN_LOAD:
                TNetworkAddress redirectAddr = GlobalStateMgr.getCurrentState()
                        .getStreamLoadMgr().executeLoadTask(
                                label, channel.getId(), request.getRequest().headers(), result, dbName, tableName);
                if (!result.stateOK() || result.containMsg()) {
                    return new ResultWrapper(result);
                }
                return new ResultWrapper(redirectAddr);
            default:
                throw new UserException("Unsupported operation: " + txnOperation);
        }
    }
}
