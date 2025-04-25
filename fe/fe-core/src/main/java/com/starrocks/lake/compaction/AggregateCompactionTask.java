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

package com.starrocks.lake.compaction;

import com.google.common.collect.Lists;
import com.starrocks.lake.compaction.CompactionTask;
import com.starrocks.proto.AbortCompactionRequest;
import com.starrocks.proto.AbortCompactionResponse;
import com.starrocks.proto.AggregateCompactRequest;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
import com.starrocks.proto.ComputeNodePB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.transaction.TabletCommitInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class AggregateCompactionTask extends CompactionTask {
    private static final Logger LOG = LogManager.getLogger(AggregateCompactionTask.class);
    private final AggregateCompactRequest request;

    public AggregateCompactionTask(long nodeId, LakeService rpcChannel, AggregateCompactRequest request) {
        super(nodeId, rpcChannel);
        this.request = Objects.requireNonNull(request, "request is null");
    }
    

    public TaskResult getResult() {
        if (!isDone()) {
            return TaskResult.NOT_FINISHED;
        }
        try {
            CompactResponse response = responseFuture.get();
            if (CollectionUtils.isEmpty(response.failedTablets)) {
                return TaskResult.ALL_SUCCESS;
            } else {
                // TODO support partial success
                return TaskResult.NONE_SUCCESS;
            }
        } catch (ExecutionException e) {
            return TaskResult.NONE_SUCCESS;
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return TaskResult.NONE_SUCCESS;
        }
    }

    public void sendRequest() {
        if (responseFuture == null) {
            responseFuture = rpcChannel.aggregateCompact(request);
        }
    }

    public void abort() {
        // no need to aggregate abort request
        TaskResult taskResult = getResult();
        if (taskResult == TaskResult.NOT_FINISHED || taskResult == TaskResult.NONE_SUCCESS) {
            // abort compaction one by one
            for (int i = 0; i < request.requests.size(); i++) {
                CompactRequest eachRequest = request.requests.get(i);
                ComputeNodePB computeNodePB = request.computeNodes.get(i);
                AbortCompactionRequest abortRequest = new AbortCompactionRequest();
                abortRequest.txnId = eachRequest.txnId;
                try {
                    LakeService service = BrpcProxy.getLakeService(computeNodePB.getHost(), computeNodePB.getBrpcPort());
                    Future<AbortCompactionResponse> ignored = service.abortCompaction(abortRequest);
                    LOG.info("abort compaction task successfully sent, txn_id: {}, node: {}", eachRequest.txnId, nodeId);
                } catch (Exception e) {
                    LOG.warn("fail to abort compaction task, txn_id: {}, node: {} error: {}", eachRequest.txnId,
                            nodeId, e.getMessage());
                }
            }
        }
    }

    public List<TabletCommitInfo> buildTabletCommitInfo() {
        List<TabletCommitInfo> tabletCommitInfo = Lists.newArrayList();
        for (int i = 0; i < request.requests.size(); i++) {
            CompactRequest eachRequest = request.requests.get(i);
            ComputeNodePB computeNodePB = request.computeNodes.get(i);
            tabletCommitInfo.addAll(eachRequest.tabletIds.stream()
                    .map(tabletId -> new TabletCommitInfo(tabletId, computeNodePB.id))
                    .collect(Collectors.toList()));
        }
        return tabletCommitInfo;
    }

    public int tabletCount() {
        int tabletCount = 0;
        for (CompactRequest request : request.requests) {
            tabletCount += request.tabletIds.size();
        }
        return tabletCount;
    }
}
