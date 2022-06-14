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
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TPublishVersionRequest;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PublishVersionTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PublishVersionTask.class);

    private long transactionId;
    private List<TPartitionVersionInfo> partitionVersionInfos;
    private List<Long> errorTablets;
    private long commitTimestamp;

    public PublishVersionTask(long backendId, long transactionId, long dbId, long commitTimestamp,
                              List<TPartitionVersionInfo> partitionVersionInfos, long createTime) {
        super(null, backendId, TTaskType.PUBLISH_VERSION, dbId, -1L, -1L, -1L, -1L, transactionId, createTime);
        this.transactionId = transactionId;
        this.partitionVersionInfos = partitionVersionInfos;
        this.errorTablets = new ArrayList<Long>();
        this.isFinished = false;
        this.commitTimestamp = commitTimestamp;
    }

    public TPublishVersionRequest toThrift() {
        TPublishVersionRequest publishVersionRequest = new TPublishVersionRequest(transactionId,
                partitionVersionInfos);
        publishVersionRequest.setCommit_timestamp(commitTimestamp);
        return publishVersionRequest;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<TPartitionVersionInfo> getPartitionVersionInfos() {
        return partitionVersionInfos;
    }

    public synchronized List<Long> getErrorTablets() {
        return errorTablets;
    }

    public synchronized void addErrorTablets(List<Long> errorTablets) {
        this.errorTablets.clear();
        if (errorTablets == null) {
            return;
        }
        this.errorTablets.addAll(errorTablets);
    }

    public void setIsFinished(boolean isFinished) {
        this.isFinished = isFinished;
    }

    public boolean isFinished() {
        return isFinished;
    }

    // collect all failed replicas for publish version task
    public Set<Long> collectErrorReplicas() {
        TabletInvertedIndex tablets = GlobalStateMgr.getCurrentInvertedIndex();
        Set<Long> errorReplicas = Sets.newHashSet();
        List<Long> errorTablets = this.getErrorTablets();
        if (errorTablets != null && !errorTablets.isEmpty()) {
            for (long tabletId : errorTablets) {
                // tablet inverted index also contains rollingup index
                // if tablet meta not found, skip it because tablet is dropped from fe
                if (tablets.getTabletMeta(tabletId) == null) {
                    continue;
                }
                Replica replica = tablets.getReplica(tabletId, this.getBackendId());
                if (replica != null) {
                    errorReplicas.add(replica.getId());
                } else {
                    LOG.info("could not find related replica with tabletid={}, backendid={}",
                            tabletId, this.getBackendId());
                }
            }
        }
        return errorReplicas;
    }
}
