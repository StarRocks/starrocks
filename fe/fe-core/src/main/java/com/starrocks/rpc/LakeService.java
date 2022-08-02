// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.brpc.protocol.BrpcMeta;
import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DeleteDataRequest;
import com.starrocks.lake.proto.DeleteDataResponse;
import com.starrocks.lake.proto.DropTabletRequest;
import com.starrocks.lake.proto.DropTabletResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;

public interface LakeService {
    @BrpcMeta(serviceName = "LakeService", methodName = "publish_version")
    PublishVersionResponse publishVersion(PublishVersionRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "abort_txn")
    AbortTxnResponse abortTxn(AbortTxnRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "compact")
    CompactResponse compact(CompactRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "drop_tablet")
    DropTabletResponse dropTablet(DropTabletRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "delete_data")
    DeleteDataResponse deleteData(DeleteDataRequest request);
}

