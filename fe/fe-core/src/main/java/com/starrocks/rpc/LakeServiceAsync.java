// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.brpc.client.RpcCallback;
import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DeleteDataRequest;
import com.starrocks.lake.proto.DeleteDataResponse;
import com.starrocks.lake.proto.DeleteTabletRequest;
import com.starrocks.lake.proto.DeleteTabletResponse;
import com.starrocks.lake.proto.DropTableRequest;
import com.starrocks.lake.proto.DropTableResponse;
import com.starrocks.lake.proto.LockTabletMetadataRequest;
import com.starrocks.lake.proto.LockTabletMetadataResponse;
import com.starrocks.lake.proto.PublishLogVersionRequest;
import com.starrocks.lake.proto.PublishLogVersionResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.lake.proto.TabletStatRequest;
import com.starrocks.lake.proto.TabletStatResponse;
import com.starrocks.lake.proto.UnlockTabletMetadataRequest;
import com.starrocks.lake.proto.UnlockTabletMetadataResponse;

import java.util.concurrent.Future;

public interface LakeServiceAsync extends LakeService {
    Future<PublishVersionResponse> publishVersion(
            PublishVersionRequest request, RpcCallback<PublishVersionResponse> callback);

    Future<AbortTxnResponse> abortTxn(AbortTxnRequest request, RpcCallback<AbortTxnResponse> callback);

    Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request, RpcCallback<DeleteTabletResponse> callback);

    Future<CompactResponse> compact(CompactRequest request, RpcCallback<CompactResponse> callback);

    Future<DeleteDataResponse> deleteData(DeleteDataRequest request, RpcCallback<DeleteDataResponse> callback);

    Future<TabletStatResponse> getTabletStats(TabletStatRequest request, RpcCallback<TabletStatResponse> callback);

    Future<DropTableResponse> dropTable(DropTableRequest request, RpcCallback<DropTableResponse> callback);

    Future<PublishLogVersionResponse> publishLogVersion(PublishLogVersionRequest request,
                                                        RpcCallback<PublishLogVersionResponse> callback);

    Future<LockTabletMetadataResponse> lockTabletMetadata(LockTabletMetadataRequest request,
                                                          RpcCallback<LockTabletMetadataResponse> callback);

    Future<UnlockTabletMetadataResponse> unlockTabletMetadata(UnlockTabletMetadataRequest request,
                                                                   RpcCallback<UnlockTabletMetadataResponse>
                                                                           callback);
}
