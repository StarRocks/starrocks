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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/DeleteJob.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load;

import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.exception.UserException;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.QueryStateException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionAlreadyCommitException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public abstract class DeleteJob extends AbstractTxnStateChangeCallback {
    private static final Logger LOG = LogManager.getLogger(DeleteJob.class);

    public enum DeleteState {
        DELETING,
        QUORUM_FINISHED,
        FINISHED
    }

    // jobId(listenerId). use in beginTransaction to callback function
    protected long id;
    // transaction id.
    protected long signature;
    protected String label;
    protected DeleteState state;
    protected MultiDeleteInfo deleteInfo;
    List<Predicate> deleteConditions;

    public DeleteJob(long id, long transactionId, String label, MultiDeleteInfo deleteInfo) {
        this.id = id;
        this.signature = transactionId;
        this.label = label;
        this.state = DeleteState.DELETING;
        this.deleteInfo = deleteInfo;
    }

    @Override
    public long getId() {
        return id;
    }

    public long getTransactionId() {
        return signature;
    }

    public String getLabel() {
        return label;
    }

    public void setState(DeleteState state) {
        this.state = state;
    }

    public DeleteState getState() {
        return state;
    }

    public MultiDeleteInfo getDeleteInfo() {
        return deleteInfo;
    }

    public List<Predicate> getDeleteConditions() {
        return deleteConditions;
    }

    public void setDeleteConditions(List<Predicate> deleteConditions) {
        this.deleteConditions = deleteConditions;
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            return;
        }
        setState(DeleteState.FINISHED);
        GlobalStateMgr.getCurrentState().getDeleteMgr().recordFinishedJob(this);
        GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getId());
        GlobalStateMgr.getCurrentState().getEditLog().logFinishMultiDelete(deleteInfo);
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason) {
        // just to clean the callback
        GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getId());
    }

    public abstract void run(DeleteStmt stmt, Database db, Table table, List<Partition> partitions)
            throws DdlException, QueryStateException;

    public abstract long getTimeoutMs();

    public abstract void clear();

    public boolean cancel(DeleteMgr.CancelType cancelType, String reason) {
        LOG.info("start to cancel delete job, transactionId: {}, cancelType: {}", getTransactionId(),
                cancelType.name());

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        try {
            globalTransactionMgr.abortTransaction(getDeleteInfo().getDbId(), getTransactionId(), reason);
        } catch (TransactionAlreadyCommitException e) {
            return false;
        } catch (Exception e) {
            LOG.warn("errors while abort transaction", e);
        }
        return true;
    }

    /**
     * commit delete job
     * return true when successfully commit and publish
     * return false when successfully commit but publish unfinished.
     * A UserException thrown if both commit and publish failed.
     */
    public abstract boolean commitImpl(Database db, long timeoutMs) throws UserException;

    public void commit(Database db, long timeoutMs) throws DdlException, QueryStateException {
        TransactionStatus status = TransactionStatus.UNKNOWN;
        try {
            if (commitImpl(db, timeoutMs)) {
                GlobalStateMgr.getCurrentState().getDeleteMgr()
                        .updateTableDeleteInfo(GlobalStateMgr.getCurrentState(), db.getId(),
                                getDeleteInfo().getTableId());
            }
            status = GlobalStateMgr.getCurrentGlobalTransactionMgr().
                    getTransactionState(db.getId(), getTransactionId()).getTransactionStatus();
        } catch (UserException e) {
            if (cancel(DeleteMgr.CancelType.COMMIT_FAIL, e.getMessage())) {
                throw new DdlException(e.getMessage(), e);
            } else {
                // do nothing
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(getLabel()).append("', 'status':'").append(status.name());
        sb.append("', 'txnId':'").append(getTransactionId()).append("'");

        switch (status) {
            case COMMITTED: {
                // Although publish is unfinished we should tell user that commit already success.
                String errMsg = "delete job is committed but may be taking effect later";
                sb.append(", 'err':'").append(errMsg).append("'");
                sb.append("}");
                throw new QueryStateException(QueryState.MysqlStateType.OK, sb.toString());
            }
            case VISIBLE: {
                sb.append("}");
                throw new QueryStateException(QueryState.MysqlStateType.OK, sb.toString());
            }
            default:
                throw new IllegalStateException("wrong transaction status: " + status.name());
        }
    }
}
