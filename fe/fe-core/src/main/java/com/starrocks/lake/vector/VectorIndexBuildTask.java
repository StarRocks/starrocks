// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.lake.vector;

import com.starrocks.proto.BuildVectorIndexRequest;
import com.starrocks.proto.BuildVectorIndexResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;

import java.util.concurrent.Future;

/**
 * Wraps an async RPC call to build vector index on a CN.
 * FE passes the committedVersion so CN loads metadata at the correct version.
 */
public class VectorIndexBuildTask {
    private final ComputeNode node;
    private final long tabletId;
    private final long version;
    private final long builtVersion;
    private final long startTimeMs;
    private Future<BuildVectorIndexResponse> future;

    public VectorIndexBuildTask(ComputeNode node, long tabletId, long version, long builtVersion) {
        this.node = node;
        this.tabletId = tabletId;
        this.version = version;
        this.builtVersion = builtVersion;
        this.startTimeMs = System.currentTimeMillis();
    }

    public void sendRequest() throws RpcException {
        BuildVectorIndexRequest request = new BuildVectorIndexRequest();
        request.tabletId = tabletId;
        request.version = version;
        request.builtVersion = builtVersion;
        LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
        future = lakeService.buildVectorIndex(request);
    }

    public boolean isDone() {
        return future != null && future.isDone();
    }

    public BuildVectorIndexResponse getResponse() throws Exception {
        BuildVectorIndexResponse response = future.get();
        if (response == null) {
            throw new RuntimeException("Build vector index returned null response");
        }
        if (response.status != null && response.status.statusCode != 0) {
            throw new RuntimeException("Build vector index failed: " + response.status.errorMsgs);
        }
        return response;
    }

    /**
     * Check if the last RPC was rejected because the CN is already building
     * this tablet (tablet-level dedup). BE returns RESOURCE_BUSY (53) for
     * this case.
     */
    public boolean isAlreadyBuilding() {
        if (future == null || !future.isDone()) {
            return false;
        }
        try {
            BuildVectorIndexResponse response = future.get();
            return response != null && response.status != null
                    && response.status.statusCode == TStatusCode.RESOURCE_BUSY.getValue();
        } catch (Exception e) {
            return false;
        }
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getVersion() {
        return version;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public ComputeNode getNode() {
        return node;
    }
}
