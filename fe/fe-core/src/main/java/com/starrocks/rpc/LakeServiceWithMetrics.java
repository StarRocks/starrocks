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
import com.starrocks.proto.AggregateCompactRequest;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.BuildVectorIndexRequest;
import com.starrocks.proto.BuildVectorIndexResponse;
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
import com.starrocks.proto.DropTabletCacheRequest;
import com.starrocks.proto.DropTabletCacheResponse;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.LockTabletMetadataRequest;
import com.starrocks.proto.LockTabletMetadataResponse;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.RepairTabletMetadataRequest;
import com.starrocks.proto.RepairTabletMetadataResponse;
import com.starrocks.proto.RestoreSnapshotsRequest;
import com.starrocks.proto.RestoreSnapshotsResponse;
import com.starrocks.proto.TabletStatRequest;
import com.starrocks.proto.TabletStatResponse;
import com.starrocks.proto.UnlockTabletMetadataRequest;
import com.starrocks.proto.UnlockTabletMetadataResponse;
import com.starrocks.proto.UploadSnapshotsRequest;
import com.starrocks.proto.UploadSnapshotsResponse;
import com.starrocks.proto.VacuumFullRequest;
import com.starrocks.proto.VacuumFullResponse;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;

import java.util.concurrent.Future;

/**
 * The decorator every lake-service caller gets: it counts the requests, and it attaches the
 * blocking-call door to the futures it hands back.
 *
 * <p>The door belongs here because this is the one place every lake RPC passes through --
 * {@link BrpcProxy#getLakeService} builds nothing else -- and because the wait that a lock would be
 * held across is the caller's {@code get()}, not the send. See {@link GuardedFuture} for why the
 * future is wrapped rather than each of the callers guarded.
 *
 * <p>Each RPC gets its own transport tag, so a slow-lock report names the request rather than the
 * service. That is deliberately finer than the operation-level tags a few call sites carry of their
 * own -- {@code com.starrocks.lake.Utils} guards its publish methods with {@code "lake-publish"} at
 * the point where the publish is decided. The two are not redundant: one says which operation
 * entered a critical section, the other says which round trip the thread is actually sitting in.
 */
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
        return GuardedFuture.guard(lakeService.publishVersion(request), "lake-publish-version");
    }

    @Override
    public Future<AbortTxnResponse> abortTxn(AbortTxnRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.abortTxn(request), "lake-abort-txn");
    }

    @Override
    public Future<CompactResponse> compact(CompactRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.compact(request), "lake-compact");
    }

    @Override
    public Future<CompactResponse> aggregateCompact(AggregateCompactRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.aggregateCompact(request), "lake-aggregate-compact");
    }

    @Override
    public Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.deleteTablet(request), "lake-delete-tablet");
    }

    @Override
    public Future<DeleteDataResponse> deleteData(DeleteDataRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.deleteData(request), "lake-delete-data");
    }

    @Override
    public Future<DeleteTxnLogResponse> deleteTxnLog(DeleteTxnLogRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.deleteTxnLog(request), "lake-delete-txn-log");
    }

    @Override
    public Future<TabletStatResponse> getTabletStats(TabletStatRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.getTabletStats(request), "lake-get-tablet-stats");
    }

    @Override
    public Future<DropTableResponse> dropTable(DropTableRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.dropTable(request), "lake-drop-table");
    }

    @Override
    public Future<DropTabletCacheResponse> dropTabletCache(DropTabletCacheRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.dropTabletCache(request), "lake-drop-tablet-cache");
    }

    @Override
    public Future<PublishLogVersionResponse> publishLogVersion(PublishLogVersionRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.publishLogVersion(request), "lake-publish-log-version");
    }

    @Override
    public Future<PublishLogVersionResponse> publishLogVersionBatch(PublishLogVersionBatchRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.publishLogVersionBatch(request), "lake-publish-log-version-batch");
    }

    @Override
    public Future<LockTabletMetadataResponse> lockTabletMetadata(LockTabletMetadataRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.lockTabletMetadata(request), "lake-lock-tablet-metadata");
    }

    @Override
    public Future<UnlockTabletMetadataResponse> unlockTabletMetadata(UnlockTabletMetadataRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.unlockTabletMetadata(request), "lake-unlock-tablet-metadata");
    }

    @Override
    public Future<UploadSnapshotsResponse> uploadSnapshots(UploadSnapshotsRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.uploadSnapshots(request), "lake-upload-snapshots");
    }

    @Override
    public Future<RestoreSnapshotsResponse> restoreSnapshots(RestoreSnapshotsRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.restoreSnapshots(request), "lake-restore-snapshots");
    }

    @Override
    public Future<AbortCompactionResponse> abortCompaction(AbortCompactionRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.abortCompaction(request), "lake-abort-compaction");
    }

    @Override
    public Future<VacuumResponse> vacuum(VacuumRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.vacuum(request), "lake-vacuum");
    }

    @Override
    public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.aggregatePublishVersion(request), "lake-aggregate-publish-version");
    }

    @Override
    public Future<VacuumFullResponse> vacuumFull(VacuumFullRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.vacuumFull(request), "lake-vacuum-full");
    }

    @Override
    public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.getTabletMetadatas(request), "lake-get-tablet-metadatas");
    }

    @Override
    public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.repairTabletMetadata(request), "lake-repair-tablet-metadata");
    }

    @Override
    public Future<BuildVectorIndexResponse> buildVectorIndex(BuildVectorIndexRequest request) {
        increaseMetrics();
        return GuardedFuture.guard(lakeService.buildVectorIndex(request), "lake-build-vector-index");
    }
}
