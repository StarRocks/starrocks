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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/PushTask.java

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

package com.starrocks.task;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TCondition;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TPriority;
import com.starrocks.thrift.TPushReq;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class PushTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushTask.class);

    private long replicaId;
    private int schemaHash;
    private long version;
    private int timeoutSecond;
    private long loadJobId;
    private TPushType pushType;
    private List<Predicate> conditions;
    // for synchronous delete
    private MarkedCountDownLatch latch;

    private TPriority priority;
    private boolean isSyncDelete;
    private long asyncDeleteJobId;

    private final long transactionId;
    private boolean isSchemaChanging;

    // for load v2 (spark load)
    private TBrokerScanRange tBrokerScanRange;
    private TDescriptorTable tDescriptorTable;
    private boolean useVectorized;
    private String timezone;

    private TTabletType tabletType;

    public PushTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, long partitionId,
                    long indexId, long tabletId, long replicaId, int schemaHash, long version,
                    int timeoutSecond, long loadJobId, TPushType pushType,
                    List<Predicate> conditions, TPriority priority, TTaskType taskType,
                    long transactionId, long signature) {
        super(resourceInfo, backendId, taskType, dbId, tableId, partitionId, indexId, tabletId, signature);
        this.replicaId = replicaId;
        this.schemaHash = schemaHash;
        this.version = version;
        this.timeoutSecond = timeoutSecond;
        this.loadJobId = loadJobId;
        this.pushType = pushType;
        this.conditions = conditions;
        this.latch = null;
        this.priority = priority;
        this.isSyncDelete = true;
        this.asyncDeleteJobId = -1;
        this.transactionId = transactionId;
        this.tBrokerScanRange = null;
        this.tDescriptorTable = null;
        this.useVectorized = true;
    }

    // for cancel delete
    public PushTask(long backendId, TPushType pushType, TPriority priority, TTaskType taskType,
                    long transactionId, long signature) {
        super(null, backendId, taskType, -1, -1, -1, -1, -1);
        this.pushType = pushType;
        this.priority = priority;
        this.transactionId = transactionId;
        this.signature = signature;
    }

    // for load v2 (SparkLoadJob)
    public PushTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                    long replicaId, int schemaHash, int timeoutSecond, long loadJobId, TPushType pushType,
                    TPriority priority, long transactionId, long signature, TBrokerScanRange tBrokerScanRange,
                    TDescriptorTable tDescriptorTable, String timezone, TTabletType tabletType) {
        this(null, backendId, dbId, tableId, partitionId, indexId,
                tabletId, replicaId, schemaHash, -1, timeoutSecond, loadJobId, pushType, null,
                priority, TTaskType.REALTIME_PUSH, transactionId, signature);
        this.tBrokerScanRange = tBrokerScanRange;
        this.tDescriptorTable = tDescriptorTable;
        this.useVectorized = true;
        this.timezone = timezone;
        this.tabletType = tabletType;
    }

    public TPushReq toThrift() {
        TPushReq request = new TPushReq(tabletId, schemaHash, version, 0, timeoutSecond, pushType);
        if (taskType == TTaskType.REALTIME_PUSH) {
            request.setPartition_id(partitionId);
            request.setTransaction_id(transactionId);
            request.setTimezone(timezone);
        }
        request.setIs_schema_changing(isSchemaChanging);
        switch (pushType) {
            case LOAD:
            case LOAD_DELETE:
                Preconditions.checkArgument(false, "LOAD and LOAD_DELETE not supported");
                break;
            case DELETE:
                List<TCondition> tConditions = new ArrayList<TCondition>();
                for (Predicate condition : conditions) {
                    TCondition tCondition = new TCondition();
                    ArrayList<String> conditionValues = new ArrayList<String>();
                    if (condition instanceof BinaryPredicate) {
                        BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                        String columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                        String value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                        Operator op = binaryPredicate.getOp();
                        tCondition.setColumn_name(columnName);
                        tCondition.setCondition_op(op.toString());
                        conditionValues.add(value);
                    } else if (condition instanceof IsNullPredicate) {
                        IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                        String columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                        String op = "IS";
                        String value = "NULL";
                        if (isNullPredicate.isNotNull()) {
                            value = "NOT NULL";
                        }
                        tCondition.setColumn_name(columnName);
                        tCondition.setCondition_op(op);
                        conditionValues.add(value);
                    } else if (condition instanceof InPredicate) {
                        InPredicate inPredicate = (InPredicate) condition;
                        String columnName = ((SlotRef) inPredicate.getChild(0)).getColumnName();
                        String op = inPredicate.isNotIn() ? "!*=" : "*=";
                        tCondition.setColumn_name(columnName);
                        tCondition.setCondition_op(op);
                        for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                            conditionValues.add(((LiteralExpr) inPredicate.getChild(i)).getStringValue());
                        }
                    }

                    tCondition.setCondition_values(conditionValues);

                    tConditions.add(tCondition);
                }
                request.setDelete_conditions(tConditions);
                request.setUse_vectorized(useVectorized);
                break;
            case LOAD_V2:
                request.setBroker_scan_range(tBrokerScanRange);
                request.setDesc_tbl(tDescriptorTable);
                request.setUse_vectorized(useVectorized);
                request.setTablet_type(tabletType);
                break;
            case CANCEL_DELETE:
                request.setTransaction_id(transactionId);
                break;
            default:
                LOG.warn("unknown push type. type: " + pushType.name());
                break;
        }

        return request;
    }

    public void setCountDownLatch(MarkedCountDownLatch latch) {
        this.latch = latch;
    }

    public void countDownLatch(long backendId, long tabletId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tabletId)) {
                LOG.info("pushTask current latch count: {}. backend: {}, tablet:{}",
                        latch.getCount(), backendId, tabletId);
            }
        }
    }

    public long getReplicaId() {
        return replicaId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getVersion() {
        return version;
    }

    public long getLoadJobId() {
        return loadJobId;
    }

    public TPushType getPushType() {
        return pushType;
    }

    public TPriority getPriority() {
        return priority;
    }

    public boolean isSyncDelete() {
        return isSyncDelete;
    }

    public long getAsyncDeleteJobId() {
        return asyncDeleteJobId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setIsSchemaChanging(boolean isSchemaChanging) {
        this.isSchemaChanging = isSchemaChanging;
    }
}
