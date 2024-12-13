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

import com.google.common.base.Preconditions;
import com.starrocks.proto.AbortCompactionRequest;
import com.starrocks.proto.AbortCompactionResponse;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
<<<<<<< HEAD
=======
import com.starrocks.proto.CompactStat;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

/**
 * CompactionTask is a subtask in {@link CompactionJob}.
 * Each CompactionTask will be executed by a single BE/CN
 * node and may include compaction tasks for multiple tablets.
 */
public class CompactionTask {
    private static final Logger LOG = LogManager.getLogger(CompactionTask.class);
    private final long nodeId;
    private final LakeService rpcChannel;
    private final CompactRequest request;
    private Future<CompactResponse> responseFuture;

<<<<<<< HEAD
=======
    // FOR TEST
    public CompactionTask(long nodeId) {
        this.nodeId = nodeId;
        this.rpcChannel = null;
        this.request = null;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public CompactionTask(long nodeId, LakeService rpcChannel, CompactRequest request) {
        this.nodeId = nodeId;
        this.rpcChannel = Objects.requireNonNull(rpcChannel, "rpcChannel is null");
        this.request = Objects.requireNonNull(request, "request is null");
<<<<<<< HEAD
=======
        this.responseFuture = null;
    }

    enum TaskResult {
      NOT_FINISHED,
      NONE_SUCCESS,
      PARTIAL_SUCCESS,
      ALL_SUCCESS
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public long getNodeId() {
        return nodeId;
    }

    public boolean isDone() {
        return responseFuture != null && responseFuture.isDone();
    }

<<<<<<< HEAD
    /**
     * Checks if compaction was completed successfully for all tablets in the task.
     *
     * @return True if compaction completed successfully for all tablets in the task
     *         False if compaction for any tablet failed or is still in progress
     */
    public boolean isCompleted() {
        return isDone() && !isFailed();
    }

    /**
     * Checks if compaction failed for any tablet in the task.
     *
     * @return True if compaction failed for any tablet in the task,
     *         False if compaction succeeded for all tablets in the task or is still in progress
     */
    public boolean isFailed() {
        if (!isDone()) {
            return false;
        }
        try {
            CompactResponse response = responseFuture.get();
            return CollectionUtils.isNotEmpty(response.failedTablets);
        } catch (ExecutionException e) {
            return true;
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return true;
=======
    public TaskResult getResult() {
        if (!isDone()) {
            return TaskResult.NOT_FINISHED;
        }
        try {
            CompactResponse response = responseFuture.get();
            if (CollectionUtils.isEmpty(response.failedTablets)) {
                return TaskResult.ALL_SUCCESS;
            } else if (response.failedTablets.size() == request.tabletIds.size()) {
                return TaskResult.NONE_SUCCESS;
            } else {
                return TaskResult.PARTIAL_SUCCESS;
            }
        } catch (ExecutionException e) {
            return TaskResult.NONE_SUCCESS;
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return TaskResult.NONE_SUCCESS;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    public String getFailMessage() {
        Preconditions.checkState(isDone());
        try {
            CompactResponse response = responseFuture.get();
            if (response.status != null && CollectionUtils.isNotEmpty(response.status.errorMsgs)) {
                return response.status.errorMsgs.get(0);
            } else if (CollectionUtils.isNotEmpty(response.failedTablets)) {
                return String.format("fail to compact tablet %d", response.failedTablets.get(0));
            } else {
                return null;
            }
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    public void sendRequest() {
        if (responseFuture == null) {
            responseFuture = rpcChannel.compact(request);
        }
    }

    public void abort() {
<<<<<<< HEAD
        if (!isCompleted()) {
=======
        TaskResult taskResult = getResult();
        if (taskResult == TaskResult.NOT_FINISHED || taskResult == TaskResult.NONE_SUCCESS) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            AbortCompactionRequest abortRequest = new AbortCompactionRequest();
            abortRequest.txnId = request.txnId;
            try {
                Future<AbortCompactionResponse> ignored = rpcChannel.abortCompaction(abortRequest);
                LOG.info("aborted compaction task, txn_id: {}, node: {}", request.txnId, nodeId);
            } catch (Exception e) {
                LOG.warn("fail to abort compaction task, txn_id: {}, node: {} error: {}", request.txnId,
                        nodeId, e.getMessage());
            }
        }
    }

    public List<TabletCommitInfo> buildTabletCommitInfo() {
        return request.tabletIds.stream().map(id -> new TabletCommitInfo(id, nodeId)).collect(Collectors.toList());
    }

    public int tabletCount() {
        return request.tabletIds.size();
    }
<<<<<<< HEAD
=======

    public List<CompactStat> getCompactStats() {
        if (!isDone()) {
            return null;
        }
        try {
            CompactResponse response = responseFuture.get();
            return response.compactStats;
        } catch (Exception e) {
            return null;
        }
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
