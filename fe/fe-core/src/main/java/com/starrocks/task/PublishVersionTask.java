// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/PublishVersionTask.java

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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.TraceManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TPublishVersionRequest;
import com.starrocks.thrift.TTabletVersionPair;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.TransactionState;
import io.opentelemetry.api.trace.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PublishVersionTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PublishVersionTask.class);

    private final long transactionId;
    private final List<TPartitionVersionInfo> partitionVersionInfos;
    private final List<Long> errorTablets;
    private Set<Long> errorReplicas;
    private final long commitTimestamp;
    private final TransactionState txnState;
    private Span span;
    private boolean enableSyncPublish;

    public PublishVersionTask(long backendId, long transactionId, long dbId, long commitTimestamp,
                              List<TPartitionVersionInfo> partitionVersionInfos, String traceParent, Span txnSpan,
                              long createTime, TransactionState state, boolean enableSyncPublish) {
        super(null, backendId, TTaskType.PUBLISH_VERSION, dbId, -1L, -1L, -1L, -1L, transactionId, createTime, traceParent);
        this.transactionId = transactionId;
        this.partitionVersionInfos = partitionVersionInfos;
        this.errorTablets = new ArrayList<>();
        this.isFinished = false;
        this.commitTimestamp = commitTimestamp;
        this.txnState = state;
        this.enableSyncPublish = enableSyncPublish;
        if (txnSpan != null) {
            span = TraceManager.startSpan("publish_version_task", txnSpan);
            span.setAttribute("backend_id", backendId);
            span.setAttribute("num_partition", partitionVersionInfos.size());
        }
    }

    public TPublishVersionRequest toThrift() {
        if (span != null) {
            span.addEvent("send_to_be");
        }
        TPublishVersionRequest publishVersionRequest = new TPublishVersionRequest(transactionId, partitionVersionInfos);
        publishVersionRequest.setCommit_timestamp(commitTimestamp);
        publishVersionRequest.setTxn_trace_parent(traceParent);
        publishVersionRequest.setEnable_sync_publish(enableSyncPublish);
        return publishVersionRequest;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public TransactionState getTxnState() {
        return txnState;
    }

    public synchronized List<Long> getErrorTablets() {
        return errorTablets;
    }

    public synchronized Set<Long> getErrorReplicas() {
        return errorReplicas;
    }

    public synchronized void setErrorTablets(List<Long> errorTablets) {
        this.errorTablets.clear();
        if (errorTablets != null) {
            this.errorTablets.addAll(errorTablets);
        }
        this.errorReplicas = collectErrorReplicas();
    }

    public void setIsFinished(boolean isFinished) {
        this.isFinished = isFinished;
        if (span != null) {
            span.setAttribute("num_error_replicas", errorReplicas.size());
            span.setAttribute("num_error_tablets", errorTablets.size());
            span.end();
        }
    }

    private Set<Long> collectErrorReplicas() {
        TabletInvertedIndex tablets = GlobalStateMgr.getCurrentInvertedIndex();
        Set<Long> errorReplicas = Sets.newHashSet();
        List<Long> errorTablets = this.getErrorTablets();
        if (errorTablets != null && !errorTablets.isEmpty()) {
            for (long tabletId : errorTablets) {
                // tablet inverted index also contains rollup index
                // if tablet meta not found, skip it because tablet is dropped from fe
                if (tablets.getTabletMeta(tabletId) == null) {
                    continue;
                }
                Replica replica = tablets.getReplica(tabletId, this.getBackendId());
                if (replica != null) {
                    errorReplicas.add(replica.getId());
                } else {
                    LOG.info("could not find related replica with tabletid={}, backendid={}", tabletId, this.getBackendId());
                }
            }
        }
        return errorReplicas;
    }

    public void updateReplicaVersions(List<TTabletVersionPair> tabletVersions) {
        if (span != null) {
            span.addEvent("update_replica_version_start");
            span.setAttribute("num_replicas", tabletVersions.size());
        }
        TabletInvertedIndex tablets = GlobalStateMgr.getCurrentInvertedIndex();
        List<Long> tabletIds = tabletVersions.stream().map(tv -> tv.tablet_id).collect(Collectors.toList());
        List<Replica> replicas = tablets.getReplicasOnBackendByTabletIds(tabletIds, backendId);
        if (replicas == null) {
            LOG.warn("backend not found or no replicas on backend, backendid={}", backendId);
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.warn("db not found dbid={}", dbId);
            return;
        }
        List<Long> droppedTablets = new ArrayList<>();
        for (int i = 0; i < tabletVersions.size(); i++) {
            if (replicas.get(i) == null) {
                droppedTablets.add(tabletVersions.get(i).tablet_id);
            }
        }
        if (!droppedTablets.isEmpty()) {
            LOG.info("during publish version some tablets were dropped(maybe by alter), tabletIds={}", droppedTablets);
        }
        db.writeLock();
        try {
            // TODO: persistent replica version
            for (int i = 0; i < tabletVersions.size(); i++) {
                TTabletVersionPair tabletVersion = tabletVersions.get(i);
                Replica replica = replicas.get(i);
                if (replica == null) {
                    continue;
                }
                replica.updateVersion(tabletVersion.version);
            }
        } finally {
            db.writeUnlock();
            if (span != null) {
                span.addEvent("update_replica_version_finish");
            }
        }
    }
}
