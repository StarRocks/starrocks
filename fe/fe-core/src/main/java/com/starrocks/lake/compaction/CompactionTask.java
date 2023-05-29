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

import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
import com.starrocks.rpc.LakeService;
import com.starrocks.transaction.TabletCommitInfo;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * CompactionTask is a subtask in {@link CompactionJob}.
 * Each CompactionTask will be executed by a single BE/CN
 * node and may include compaction tasks for multiple tablets.
 */
public class CompactionTask {
    private final long nodeId;
    private final LakeService rpcChannel;
    private final CompactRequest request;
    private Future<CompactResponse> responseFuture;

    public CompactionTask(long nodeId, LakeService rpcChannel, CompactRequest request) {
        this.nodeId = nodeId;
        this.rpcChannel = Objects.requireNonNull(rpcChannel, "rpcChannel is null");
        this.request = Objects.requireNonNull(request, "request is null");
    }

    public long getNodeId() {
        return nodeId;
    }

    public Future<CompactResponse> getResponseFuture() {
        return responseFuture;
    }

    public boolean isDone() {
        return responseFuture != null && responseFuture.isDone();
    }

    public void sendRequest() {
        if (responseFuture == null) {
            responseFuture = rpcChannel.compact(request);
        }
    }

    public List<TabletCommitInfo> buildTabletCommitInfo() {
        return request.tabletIds.stream().map(id -> new TabletCommitInfo(id, nodeId)).collect(Collectors.toList());
    }

    public int tabletCount() {
        return request.tabletIds.size();
    }
}
