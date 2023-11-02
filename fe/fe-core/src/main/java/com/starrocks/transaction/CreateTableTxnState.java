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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/TransactionState.java

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

package com.starrocks.transaction;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AbortCreateTableTxnTask;
import com.starrocks.task.AgentTaskExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CreateTableTxnState {
    private static final Logger LOG = LogManager.getLogger(CreateTableTxnState.class);

    public enum TxnState {
        INITIAL,
        PREPARED,
        COMMITTED,
        ABORTED
    }

    private Long txnId;

    // the transaction timeout, the unit is second
    private int txnTimeOut;

    private Long txnStartTime;

    private TxnState txnState;

    public Long getTxnId() {
        return txnId;
    }

    public CreateTableTxnState(Long txnId, int txnTimeOut) {
        this.txnId = txnId;
        this.txnState = TxnState.INITIAL;
        this.txnStartTime = System.currentTimeMillis() / 1000;
    }

    public boolean isTimeout(Long currentTime) {
        return (currentTime - txnStartTime) > txnTimeOut;
    }

    public void abort() {
        if (txnState != TxnState.PREPARED) {
            return;
        }
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task to all backends.
        List<Long> allBeIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false);
        for (Long beId : allBeIds) {
            AbortCreateTableTxnTask task = new AbortCreateTableTxnTask(txnId, beId);
            AgentTaskExecutor.submit(task);
        }
    }
}
