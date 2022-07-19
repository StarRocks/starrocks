// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.brpc.client.RpcCallback;
import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DropTabletRequest;
import com.starrocks.lake.proto.DropTabletResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;

import java.util.concurrent.Future;

public interface LakeServiceAsync extends LakeService  {
    Future<PublishVersionResponse> publishVersion(
            PublishVersionRequest request, RpcCallback<PublishVersionResponse> callback);

    Future<AbortTxnResponse> abortTxn(AbortTxnRequest request, RpcCallback<AbortTxnResponse> callback);

    Future<DropTabletResponse> dropTablet(DropTabletRequest request, RpcCallback<DropTabletResponse> callback);

    Future<CompactResponse> compact(CompactRequest request, RpcCallback<CompactRequest> callback);
}