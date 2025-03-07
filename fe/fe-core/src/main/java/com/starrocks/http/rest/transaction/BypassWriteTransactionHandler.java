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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.http.rest.transaction.TransactionOperationParams.Body;
import com.starrocks.load.loadv2.MiniLoadTxnCommitAttachment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.transaction.TransactionStatus;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.starrocks.catalog.FunctionSet.HOST_NAME;
import static com.starrocks.transaction.TransactionState.LoadJobSourceType.BYPASS_WRITE;

/**
 * Transaction management request handler for bypass writing scenario.
 */
public class BypassWriteTransactionHandler implements TransactionOperationHandler {

    private static final Logger LOG = LogManager.getLogger(BypassWriteTransactionHandler.class);

    private final TransactionOperationParams txnOperationParams;

    public BypassWriteTransactionHandler(TransactionOperationParams txnOperationParams) {
        Validate.isTrue(txnOperationParams.getChannel().isNull(), "channel isn't null");
        this.txnOperationParams = txnOperationParams;
    }

    @Override
    public ResultWrapper handle(BaseRequest request, BaseResponse response) throws StarRocksException {
        TransactionOperation txnOperation = txnOperationParams.getTxnOperation();
        String dbName = txnOperationParams.getDbName();
        String tableName = txnOperationParams.getTableName();
        String label = txnOperationParams.getLabel();
        LoadJobSourceType sourceType = txnOperationParams.getSourceType();
        Long timeoutMillis = txnOperationParams.getTimeoutMillis();
        Body requestBody = txnOperationParams.getBody();
        LOG.info("Handle bypass write transaction, label: {}", label);

        Database db = Optional.ofNullable(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName))
                .orElseThrow(() -> new StarRocksException(String.format("Database[%s] does not exist.", dbName)));

        TransactionResult result;
        switch (txnOperation) {
            case TXN_BEGIN:
                Table table = Optional.ofNullable(GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(db.getFullName(), tableName))
                            .orElseThrow(() -> new StarRocksException(
                                String.format("Table[%s.%s] does not exist.", dbName, tableName)));
                result = handleBeginTransaction(db, table, label, sourceType, timeoutMillis);
                break;
            case TXN_PREPARE:
                result = handlePrepareTransaction(
                        db, label,
                        Optional.ofNullable(requestBody.getCommittedTablets()).orElse(new ArrayList<>(0)),
                        Optional.ofNullable(requestBody.getFailedTablets()).orElse(new ArrayList<>(0)),
                        timeoutMillis
                );
                break;
            case TXN_COMMIT:
                result = handleCommitTransaction(db, label, timeoutMillis);
                break;
            case TXN_ROLLBACK:
                result = handleRollbackTransaction(
                        db, label,
                        Optional.ofNullable(requestBody.getFailedTablets()).orElse(new ArrayList<>(0))
                );
                break;
            default:
                throw new StarRocksException("Unsupported operation: " + txnOperation);
        }

        return new ResultWrapper(result);
    }

    private TransactionResult handleBeginTransaction(Database db,
                                                     Table table,
                                                     String label,
                                                     LoadJobSourceType sourceType,
                                                     long timeoutMillis) throws StarRocksException {
        long dbId = db.getId();
        long tableId = table.getId();

        TxnCoordinator coordinator = new TxnCoordinator(TxnSourceType.FE, HOST_NAME);
        GlobalTransactionMgr globalTxnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long txnId = globalTxnMgr.beginTransaction(
                dbId, Lists.newArrayList(tableId), label, coordinator, sourceType, timeoutMillis);
        TransactionResult result = new TransactionResult();
        result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
        result.addResultEntry(TransactionResult.LABEL_KEY, label);
        return result;
    }

    private TransactionResult handlePrepareTransaction(Database db,
                                                       String label,
                                                       List<TabletCommitInfo> committedTablets,
                                                       List<TabletFailInfo> failedTablets,
                                                       long timeoutMillis) throws StarRocksException {
        long dbId = db.getId();
        TransactionState txnState = getTxnState(dbId, label);
        long txnId = txnState.getTransactionId();
        TransactionStatus txnStatus = txnState.getTransactionStatus();
        TransactionResult result = new TransactionResult();
        switch (txnStatus) {
            case PREPARE:
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().prepareTransaction(
                        dbId, txnId, committedTablets, failedTablets, new MiniLoadTxnCommitAttachment(), timeoutMillis);
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case PREPARED:
            case COMMITTED:
            case VISIBLE:
                result.setOKMsg(String.format("Transaction %s has already %s, label is %s", txnId, txnStatus, label));
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            default:
                throw new StarRocksException(String.format(
                        "Can not prepare %s transaction %d, label is %s", txnStatus, txnId, label));
        }
        return result;
    }

    private TransactionResult handleCommitTransaction(Database db,
                                                      String label,
                                                      long timeoutMillis) throws StarRocksException {
        long dbId = db.getId();
        TransactionState txnState = getTxnState(dbId, label);
        long txnId = txnState.getTransactionId();
        TransactionStatus txnStatus = txnState.getTransactionStatus();
        TransactionResult result = new TransactionResult();
        switch (txnStatus) {
            case PREPARED:
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .commitPreparedTransaction(db.getId(), txnId, timeoutMillis);
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case COMMITTED:
            case VISIBLE:
                result.setOKMsg(String.format("Transaction %s has already committed, label is %s", txnId, label));
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            default:
                throw new StarRocksException(String.format(
                        "Can not commit %s transaction %s, label is %s", txnStatus, txnId, label));
        }

        return result;
    }

    private TransactionResult handleRollbackTransaction(Database db,
                                                        String label,
                                                        List<TabletFailInfo> failedTablets) throws StarRocksException {
        long dbId = db.getId();
        TransactionState txnState = getTxnState(dbId, label);
        long txnId = txnState.getTransactionId();
        TransactionStatus txnStatus = txnState.getTransactionStatus();
        TransactionResult result = new TransactionResult();
        switch (txnStatus) {
            case PREPARED:
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .abortTransaction(dbId, txnId, "User Aborted", failedTablets);
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case ABORTED:
                result.setOKMsg(String.format("Transaction %s has already aborted, label is %s", txnId, label));
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            default:
                throw new StarRocksException(String.format(
                        "Can not abort %s transaction %s, label is %s", txnStatus, txnId, label));
        }

        return result;
    }

    private static TransactionState getTxnState(long dbId, String label) throws StarRocksException {
        TransactionState txnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getLabelTransactionState(dbId, label);
        if (null == txnState) {
            throw new StarRocksException(String.format("No transaction found by label %s", label));
        }

        if (BYPASS_WRITE.equals(txnState.getSourceType())) {
            return txnState;
        }

        throw new StarRocksException(String.format(
                "Transaction found by label %s isn't created in %s scenario.", label, BYPASS_WRITE.name()));
    }

}
