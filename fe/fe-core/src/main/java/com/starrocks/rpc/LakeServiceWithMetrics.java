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
package com.starrocks.rpc;

import com.starrocks.metric.MetricRepo;
import com.starrocks.proto.AbortCompactionRequest;
import com.starrocks.proto.AbortCompactionResponse;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.proto.AbortTxnResponse;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
import com.starrocks.proto.DeleteDataRequest;
import com.starrocks.proto.DeleteDataResponse;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.proto.DeleteTxnLogRequest;
import com.starrocks.proto.DeleteTxnLogResponse;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.proto.LockTabletMetadataRequest;
import com.starrocks.proto.LockTabletMetadataResponse;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.RestoreSnapshotsRequest;
import com.starrocks.proto.RestoreSnapshotsResponse;
import com.starrocks.proto.TabletStatRequest;
import com.starrocks.proto.TabletStatResponse;
import com.starrocks.proto.UnlockTabletMetadataRequest;
import com.starrocks.proto.UnlockTabletMetadataResponse;
import com.starrocks.proto.UploadSnapshotsRequest;
import com.starrocks.proto.UploadSnapshotsResponse;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;

import java.util.concurrent.Future;

public class LakeServiceWithMetrics implements LakeService {
    final LakeService lakeService;
    public LakeServiceWithMetrics(LakeService lakeService) {
        this.lakeService = lakeService;
    }

    private static void increaseMetrics() {
        if (MetricRepo.COUNTER_LAKE_SERVICE_RPC != null) {
            MetricRepo.COUNTER_LAKE_SERVICE_RPC.increase(1L);
        }
    }

    @Override
    public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
        increaseMetrics();
        return lakeService.publishVersion(request);
    }

    @Override
    public Future<AbortTxnResponse> abortTxn(AbortTxnRequest request) {
        increaseMetrics();
        return lakeService.abortTxn(request);
    }

    @Override
    public Future<CompactResponse> compact(CompactRequest request) {
        increaseMetrics();
        return lakeService.compact(request);
    }

    @Override
    public Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) {
        increaseMetrics();
        return lakeService.deleteTablet(request);
    }

    @Override
    public Future<DeleteDataResponse> deleteData(DeleteDataRequest request) {
        increaseMetrics();
        return lakeService.deleteData(request);
    }

    @Override
    public Future<DeleteTxnLogResponse> deleteTxnLog(DeleteTxnLogRequest request) {
        increaseMetrics();
        return lakeService.deleteTxnLog(request);
    }

    @Override
    public Future<TabletStatResponse> getTabletStats(TabletStatRequest request) {
        increaseMetrics();
        return lakeService.getTabletStats(request);
    }

    @Override
    public Future<DropTableResponse> dropTable(DropTableRequest request) {
        increaseMetrics();
        return lakeService.dropTable(request);
    }

    @Override
    public Future<PublishLogVersionResponse> publishLogVersion(PublishLogVersionRequest request) {
        increaseMetrics();
        return lakeService.publishLogVersion(request);
    }

    @Override
    public Future<PublishLogVersionResponse> publishLogVersionBatch(PublishLogVersionBatchRequest request) {
        increaseMetrics();
        return lakeService.publishLogVersionBatch(request);
    }

    @Override
    public Future<LockTabletMetadataResponse> lockTabletMetadata(LockTabletMetadataRequest request) {
        increaseMetrics();
        return lakeService.lockTabletMetadata(request);
    }

    @Override
    public Future<UnlockTabletMetadataResponse> unlockTabletMetadata(UnlockTabletMetadataRequest request) {
        increaseMetrics();
        return lakeService.unlockTabletMetadata(request);
    }

    @Override
    public Future<UploadSnapshotsResponse> uploadSnapshots(UploadSnapshotsRequest request) {
        increaseMetrics();
        return lakeService.uploadSnapshots(request);
    }

    @Override
    public Future<RestoreSnapshotsResponse> restoreSnapshots(RestoreSnapshotsRequest request) {
        increaseMetrics();
        return lakeService.restoreSnapshots(request);
    }

    @Override
    public Future<AbortCompactionResponse> abortCompaction(AbortCompactionRequest request) {
        increaseMetrics();
        return lakeService.abortCompaction(request);
    }

    @Override
    public Future<VacuumResponse> vacuum(VacuumRequest request) {
        increaseMetrics();
        return lakeService.vacuum(request);
    }
}
