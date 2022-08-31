// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DeleteDataRequest;
import com.starrocks.lake.proto.DeleteDataResponse;
import com.starrocks.lake.proto.DeleteTabletRequest;
import com.starrocks.lake.proto.DeleteTabletResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.lake.proto.TabletStatRequest;
import com.starrocks.lake.proto.TabletStatResponse;

import java.util.concurrent.Future;

public interface LakeService {
    @ProtobufRPC(serviceName = "LakeService", methodName = "publish_version", onceTalkTimeout = 5000)
    Future<PublishVersionResponse> publishVersionAsync(PublishVersionRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "abort_txn", onceTalkTimeout = 5000)
    Future<AbortTxnResponse> abortTxnAsync(AbortTxnRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "compact", onceTalkTimeout = 1800000)
    Future<CompactResponse> compactAsync(CompactRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "delete_tablet", onceTalkTimeout = 5000)
    Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "delete_data", onceTalkTimeout = 5000)
    Future<DeleteDataResponse> deleteData(DeleteDataRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "get_tablet_stats", onceTalkTimeout = 5000)
    Future<TabletStatResponse> getTabletStats(TabletStatRequest request);
}

