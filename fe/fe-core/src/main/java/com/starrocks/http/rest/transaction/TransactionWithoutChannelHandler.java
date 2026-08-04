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

import com.starrocks.catalog.Database;
import com.starrocks.common.StarRocksException;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStateSnapshot;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Transaction management request handler without channel info (eg. id, num) in request.
 */
public class TransactionWithoutChannelHandler implements TransactionOperationHandler {

    private static final Logger LOG = LogManager.getLogger(TransactionWithoutChannelHandler.class);

    private final TransactionOperationParams txnOperationParams;

    // Maybe null because only begin operation need it, and it's lazily initialized in handle() method
    @Nullable
    private Warehouse warehouse;

    public TransactionWithoutChannelHandler(TransactionOperationParams txnOperationParams) {
        Validate.isTrue(txnOperationParams.getChannel().isNull(), "channel isn't null");
        this.txnOperationParams = txnOperationParams;
    }

    public TransactionOperationParams getTxnOperationParams() {
        return txnOperationParams;
    }

    public Optional<Warehouse> getWarehouse() {
        return Optional.ofNullable(warehouse);
    }

    @Override
    public ResultWrapper handle(BaseRequest request, BaseResponse response) throws StarRocksException {
        TransactionOperation txnOperation = txnOperationParams.getTxnOperation();
        String dbName = txnOperationParams.getDbName();
        String label = txnOperationParams.getLabel();
        Long timeoutMillis = txnOperationParams.getTimeoutMillis();
        LOG.info("Handle transaction without channel info, label: {}", label);

        Database db = Optional.ofNullable(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName))
                .orElseThrow(() -> new StarRocksException(String.format("Database[%s] does not exist.", dbName)));

        TransactionResult result = null;
        switch (txnOperation) {
            case TXN_BEGIN:
            case TXN_LOAD:
                break;
            case TXN_PREPARE:
                result = handlePrepareTransaction(db, label);
                break;
            case TXN_COMMIT:
                result = handleCommitTransaction(db, label, timeoutMillis);
                break;
            case TXN_ROLLBACK:
                result = handleRollbackTransaction(db, label);
                break;
            default:
                throw new StarRocksException("Unsupported operation: " + txnOperation);
        }

        return new ResultWrapper(result);
    }

    // A PREPARE of a live transaction is handled by redirecting to the BE (result stays null). But a
    // PREPARE retry of a count-evicted transaction is routed here even for a bypass-write client
    // (source_type is omitted on retry), and redirecting it through the now-expired coordinator cache
    // would fail. Unlike the bypass-write handler this path is source-agnostic on the live path too, so
    // it answers regardless of the cached source type. When the full state is gone, answer idempotently
    // from the terminal-state cache, exactly like a commit retry: a VISIBLE/COMMITTED outcome is success,
    // ABORTED fails, and a truly unknown label is not found.
    private TransactionResult handlePrepareTransaction(Database db, String label) throws StarRocksException {
        long dbId = db.getId();
        // Decide purely from getLabelStatus (do NOT call getLabelTransactionState here: the normal
        // PREPARE redirect path relies on its own call to that method, and an extra one would disturb
        // it). A terminal (VISIBLE/COMMITTED/ABORTED) status means the full state is gone but the
        // outcome is cached -> answer the retry idempotently, exactly like a commit retry. PREPARED
        // (live) or UNKNOWN (state lives on the coordinator BE) fall through to a null result -> BE
        // redirect, preserving the ordinary stream-load PREPARE path.
        TransactionStatus status =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getLabelStatus(dbId, label).getStatus();
        if (status == TransactionStatus.VISIBLE || status == TransactionStatus.COMMITTED
                || status == TransactionStatus.ABORTED) {
            return commitEvictedTransactionByLabel(dbId, label);
        }
        return null;
    }

    private TransactionResult handleCommitTransaction(Database db,
                                                      String label,
                                                      long timeoutMillis) throws StarRocksException {
        long dbId = db.getId();
        TransactionState txnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getLabelTransactionState(dbId, label);
        if (txnState == null) {
            // The full transaction state may have been evicted (count-based eviction ignores age).
            // Resolve the terminal outcome from the cache by label so a savepoint/resume recommit is
            // answered definitively instead of "no transaction found".
            return commitEvictedTransactionByLabel(dbId, label);
        }
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
            case ABORTED:
                throw new StarRocksException(String.format(
                        "Can not commit %s transaction %s, label is %s", txnStatus, txnId, label));
            default:
                return null;
        }

        return result;
    }

    private TransactionResult handleRollbackTransaction(Database db,
                                                        String label) throws StarRocksException {
        long dbId = db.getId();
        TransactionState txnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getLabelTransactionState(dbId, label);
        if (txnState == null) {
            // Evicted (see handleCommitTransaction); resolve the terminal outcome from the cache.
            return rollbackEvictedTransactionByLabel(dbId, label);
        }
        long txnId = txnState.getTransactionId();
        TransactionStatus txnStatus = txnState.getTransactionStatus();
        TransactionResult result = new TransactionResult();
        switch (txnStatus) {
            case PREPARED:
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .abortTransaction(dbId, txnId, "User Aborted");
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case ABORTED:
                result.setOKMsg(String.format("Transaction %s has already aborted, label is %s", txnId, label));
                result.addResultEntry(TransactionResult.TXN_ID_KEY, txnId);
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case COMMITTED:
            case VISIBLE:
                throw new StarRocksException(String.format(
                        "Can not abort %s transaction %s, label is %s", txnStatus, txnId, label));
            default:
                return null;
        }

        return result;
    }

    // Resolve a commit for a transaction whose full state has been evicted, from the terminal-state
    // cache (by label). VISIBLE/COMMITTED -> idempotent success; ABORTED -> cannot-commit; otherwise
    // genuinely unknown -> not found. Mirrors the in-map status branches in handleCommitTransaction.
    private static TransactionResult commitEvictedTransactionByLabel(long dbId, String label) throws StarRocksException {
        TransactionStateSnapshot snapshot =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getLabelStatus(dbId, label);
        TransactionResult result = new TransactionResult();
        switch (snapshot.getStatus()) {
            case COMMITTED:
            case VISIBLE:
                result.setOKMsg(String.format("Transaction has already committed, label is %s", label));
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case ABORTED:
                throw new StarRocksException(
                        String.format("Can not commit ABORTED transaction, label is %s", label));
            default:
                throw new StarRocksException(String.format("No transaction found by label %s", label));
        }
        return result;
    }

    // Rollback counterpart of commitEvictedTransactionByLabel.
    private static TransactionResult rollbackEvictedTransactionByLabel(long dbId, String label)
            throws StarRocksException {
        TransactionStateSnapshot snapshot =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getLabelStatus(dbId, label);
        TransactionResult result = new TransactionResult();
        switch (snapshot.getStatus()) {
            case ABORTED:
                result.setOKMsg(String.format("Transaction has already aborted, label is %s", label));
                result.addResultEntry(TransactionResult.LABEL_KEY, label);
                break;
            case COMMITTED:
            case VISIBLE:
                throw new StarRocksException(
                        String.format("Can not abort COMMITTED transaction, label is %s", label));
            default:
                throw new StarRocksException(String.format("No transaction found by label %s", label));
        }
        return result;
    }

}
