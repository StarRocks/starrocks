// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;

import java.util.concurrent.Future;

public interface LakeService {
    @ProtobufRPC(serviceName = "LakeService", methodName = "publish_version", onceTalkTimeout = 5000)
    Future<PublishVersionResponse> publishVersionAsync(PublishVersionRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "abort_txn", onceTalkTimeout = 5000)
    Future<AbortTxnResponse> abortTxnAsync(AbortTxnRequest request);
}

