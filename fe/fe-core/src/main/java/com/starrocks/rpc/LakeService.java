// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
import com.starrocks.lake.proto.DropTableRequest;
import com.starrocks.lake.proto.DropTableResponse;
import com.starrocks.lake.proto.LockTabletMetadataRequest;
import com.starrocks.lake.proto.LockTabletMetadataResponse;
import com.starrocks.lake.proto.PublishLogVersionRequest;
import com.starrocks.lake.proto.PublishLogVersionResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.lake.proto.RestoreSnapshotsRequest;
import com.starrocks.lake.proto.RestoreSnapshotsResponse;
import com.starrocks.lake.proto.TabletStatRequest;
import com.starrocks.lake.proto.TabletStatResponse;
import com.starrocks.lake.proto.UnlockTabletMetadataRequest;
import com.starrocks.lake.proto.UnlockTabletMetadataResponse;
import com.starrocks.lake.proto.UploadSnapshotsRequest;
import com.starrocks.lake.proto.UploadSnapshotsResponse;

import java.util.concurrent.Future;

public interface LakeService {
    @ProtobufRPC(serviceName = "LakeService", methodName = "publish_version", onceTalkTimeout = 5000)
    Future<PublishVersionResponse> publishVersion(PublishVersionRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "abort_txn", onceTalkTimeout = 5000)
    Future<AbortTxnResponse> abortTxn(AbortTxnRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "compact", onceTalkTimeout = 86400000)
    Future<CompactResponse> compact(CompactRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "delete_tablet", onceTalkTimeout = 5000)
    Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "delete_data", onceTalkTimeout = 5000)
    Future<DeleteDataResponse> deleteData(DeleteDataRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "get_tablet_stats", onceTalkTimeout = 5000)
    Future<TabletStatResponse> getTabletStats(TabletStatRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "drop_table", onceTalkTimeout = 5000)
    Future<DropTableResponse> dropTable(DropTableRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "publish_log_version", onceTalkTimeout = 5000)
    Future<PublishLogVersionResponse> publishLogVersion(PublishLogVersionRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "lock_tablet_metadata", onceTalkTimeout = 5000)
    Future<LockTabletMetadataResponse> lockTabletMetadata(LockTabletMetadataRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "unlock_tablet_metadata", onceTalkTimeout = 5000)
    Future<UnlockTabletMetadataResponse> unlockTabletMetadata(UnlockTabletMetadataRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "upload_snapshots", onceTalkTimeout = 5000)
    Future<UploadSnapshotsResponse> uploadSnapshots(UploadSnapshotsRequest request);

    @ProtobufRPC(serviceName = "LakeService", methodName = "restore_snapshots", onceTalkTimeout = 5000)
    Future<RestoreSnapshotsResponse> restoreSnapshots(RestoreSnapshotsRequest request);
}

