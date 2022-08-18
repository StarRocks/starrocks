// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.brpc.protocol.BrpcMeta;
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
import com.starrocks.lake.proto.PublishLogVersionRequest;
import com.starrocks.lake.proto.PublishLogVersionResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.lake.proto.TabletStatRequest;
import com.starrocks.lake.proto.TabletStatResponse;

public interface LakeService {
    @BrpcMeta(serviceName = "LakeService", methodName = "publish_version")
    PublishVersionResponse publishVersion(PublishVersionRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "abort_txn")
    AbortTxnResponse abortTxn(AbortTxnRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "compact")
    CompactResponse compact(CompactRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "delete_tablet")
    DeleteTabletResponse deleteTablet(DeleteTabletRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "delete_data")
    DeleteDataResponse deleteData(DeleteDataRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "get_tablet_stats")
    TabletStatResponse getTabletStats(TabletStatRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "drop_table")
    DropTableResponse dropTable(DropTableRequest request);

    @BrpcMeta(serviceName = "LakeService", methodName = "publish_log_version")
    PublishLogVersionResponse publish_log_version(PublishLogVersionRequest request);
}

