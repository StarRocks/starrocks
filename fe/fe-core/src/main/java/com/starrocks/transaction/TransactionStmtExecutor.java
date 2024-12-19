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

import com.starrocks.lake.LakeTableHelper;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransactionStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(TransactionStmtExecutor.class);

    public static void beginStmt(ConnectContext context, BeginStmt stmt) throws BeginTransactionException {
        long transactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionIDGenerator().getNextTransactionId();
        String label = MetaUtils.genInsertLabel(context.getExecutionId());
        TransactionState transactionState = new TransactionState(
                transactionId, label, null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout());
        transactionState.setPrepareTime(System.currentTimeMillis());
        transactionState.setWarehouseId(context.getCurrentWarehouseId());
        boolean combinedTxnLog = LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING);
        transactionState.setUseCombinedTxnLog(combinedTxnLog);

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);
        context.setExplicitTxnState(explicitTxnState);

        context.getState().setOk(0, 0, buildMessage(label, TransactionStatus.PREPARE, transactionId, -1));
    }

    public static void commitStmt(ConnectContext context, CommitStmt stmt) {
        context.setExplicitTxnState(null);
        //TODO: commit
    }

    public static void rollbackStmt(ConnectContext context, RollbackStmt stmt) {
        context.setExplicitTxnState(null);
        //TODO: rollback
    }

    public static String buildMessage(String label, TransactionStatus txnStatus, long transactionId, long databaseId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("'label':'").append(label).append("', ");
        sb.append("'status':'").append(txnStatus.name()).append("', ");
        sb.append("'txnId':'").append(transactionId).append("'");

        if (txnStatus == TransactionStatus.COMMITTED) {
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            String timeoutInfo = transactionMgr.getTxnPublishTimeoutDebugInfo(databaseId, transactionId);
            LOG.warn("txn {} publish timeout {}", transactionId, timeoutInfo);
            if (timeoutInfo.length() > 240) {
                timeoutInfo = timeoutInfo.substring(0, 240) + "...";
            }
            String errMsg = "Publish timeout " + timeoutInfo;

            sb.append(", 'err':'").append(errMsg).append("'");
        }

        sb.append("}");

        return sb.toString();
    }
}
