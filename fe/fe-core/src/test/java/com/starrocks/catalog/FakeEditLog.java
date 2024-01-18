// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/FakeEditLog.java

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

package com.starrocks.catalog;

import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.BatchAlterJobPersistInfo;
import com.starrocks.cluster.Cluster;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.system.Backend;
import com.starrocks.transaction.TransactionState;
import mockit.Mock;
import mockit.MockUp;

import java.util.HashMap;
import java.util.Map;

public class FakeEditLog extends MockUp<EditLog> {

    private Map<Long, TransactionState> allTransactionState = new HashMap<>();

    @Mock
    public void init(String nodeName) {
    }

    @Mock
    public void logInsertTransactionState(TransactionState transactionState) {
        allTransactionState.put(transactionState.getTransactionId(), transactionState);
    }

    @Mock
    public void logDeleteTransactionState(TransactionState transactionState) {
        allTransactionState.remove(transactionState.getTransactionId());
    }

    @Mock
    public void logSaveNextId(long nextId) {
    }

    @Mock
    public void logCreateCluster(Cluster cluster) {
    }

    @Mock
    public void logOpRoutineLoadJob(RoutineLoadOperation operation) {
    }

    @Mock
    public void logBackendStateChange(Backend be) {
    }

    @Mock
    public void logAlterJob(AlterJobV2 alterJob) {

    }

    @Mock
    public void logBatchAlterJob(BatchAlterJobPersistInfo batchAlterJobV2) {

    }

    @Mock
    public void logDynamicPartition(ModifyTablePropertyOperationLog info) {

    }

    @Mock
    public void logAddReplica(ReplicaPersistInfo info) {

    }

    public TransactionState getTransaction(long transactionId) {
        return allTransactionState.get(transactionId);
    }
}
