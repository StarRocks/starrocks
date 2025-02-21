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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;

public class LakeServiceWithMetricsTest {
    @Tested
    LakeServiceWithMetrics lakeServiceWithMetrics;

    @Injectable
    LakeService lakeService;

    @BeforeClass
    public static void setUp() {
        MetricRepo.init();
    }

    @Test
    public void testPublishVersion() throws Exception {
        new Expectations() {
            {
                lakeService.publishVersion((PublishVersionRequest) any);
                result = CompletableFuture.completedFuture(new PublishVersionResponse());
            }
        };

        Future<PublishVersionResponse> result = lakeServiceWithMetrics.publishVersion(new PublishVersionRequest());
        assertNotNull(result);
    }

    @Test
    public void testAbortTxn() throws Exception {
        new Expectations() {
            {
                lakeService.abortTxn((AbortTxnRequest) any);
                result = CompletableFuture.completedFuture(new AbortTxnResponse());
            }
        };

        Future<AbortTxnResponse> result = lakeServiceWithMetrics.abortTxn(new AbortTxnRequest());
        assertNotNull(result);
    }

    @Test
    public void testCompact() throws Exception {
        new Expectations() {
            {
                lakeService.compact((CompactRequest) any);
                result = CompletableFuture.completedFuture(new CompactResponse());
            }
        };

        Future<CompactResponse> result = lakeServiceWithMetrics.compact(new CompactRequest());
        assertNotNull(result);
    }

    @Test
    public void testDeleteTablet() throws Exception {
        new Expectations() {
            {
                lakeService.deleteTablet((DeleteTabletRequest) any);
                result = CompletableFuture.completedFuture(new DeleteTabletResponse());
            }
        };

        Future<DeleteTabletResponse> result = lakeServiceWithMetrics.deleteTablet(new DeleteTabletRequest());
        assertNotNull(result);
    }

    @Test
    public void testDeleteData() throws Exception {
        new Expectations() {
            {
                lakeService.deleteData((DeleteDataRequest) any);
                result = CompletableFuture.completedFuture(new DeleteDataResponse());
            }
        };

        Future<DeleteDataResponse> result = lakeServiceWithMetrics.deleteData(new DeleteDataRequest());
        assertNotNull(result);
    }

    @Test
    public void testDeleteTxnLog() throws Exception {
        new Expectations() {
            {
                lakeService.deleteTxnLog((DeleteTxnLogRequest) any);
                result = CompletableFuture.completedFuture(new DeleteTxnLogResponse());
            }
        };

        Future<DeleteTxnLogResponse> result = lakeServiceWithMetrics.deleteTxnLog(new DeleteTxnLogRequest());
        assertNotNull(result);
    }

    @Test
    public void testGetTabletStats() throws Exception {
        new Expectations() {
            {
                lakeService.getTabletStats((TabletStatRequest) any);
                result = CompletableFuture.completedFuture(new TabletStatResponse());
            }
        };

        Future<TabletStatResponse> result = lakeServiceWithMetrics.getTabletStats(new TabletStatRequest());
        assertNotNull(result);
    }

    @Test
    public void testDropTable() throws Exception {
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                result = CompletableFuture.completedFuture(new DropTableResponse());
            }
        };

        Future<DropTableResponse> result = lakeServiceWithMetrics.dropTable(new DropTableRequest());
        assertNotNull(result);
    }

    @Test
    public void testPublishLogVersion() throws Exception {
        new Expectations() {
            {
                lakeService.publishLogVersion((PublishLogVersionRequest) any);
                result = CompletableFuture.completedFuture(new PublishLogVersionResponse());
            }
        };

        Future<PublishLogVersionResponse> result = lakeServiceWithMetrics.publishLogVersion(new PublishLogVersionRequest());
        assertNotNull(result);
    }

    @Test
    public void testPublishLogVersionBatch() throws Exception {
        new Expectations() {
            {
                lakeService.publishLogVersionBatch((PublishLogVersionBatchRequest) any);
                result = CompletableFuture.completedFuture(new PublishLogVersionResponse());
            }
        };

        Future<PublishLogVersionResponse> result =
                lakeServiceWithMetrics.publishLogVersionBatch(new PublishLogVersionBatchRequest());
        assertNotNull(result);
    }

    @Test
    public void testLockTabletMetadata() throws Exception {
        new Expectations() {
            {
                lakeService.lockTabletMetadata((LockTabletMetadataRequest) any);
                result = CompletableFuture.completedFuture(new LockTabletMetadataResponse());
            }
        };

        Future<LockTabletMetadataResponse> result =
                lakeServiceWithMetrics.lockTabletMetadata(new LockTabletMetadataRequest());
        assertNotNull(result);
    }

    @Test
    public void testUnlockTabletMetadata() throws Exception {
        new Expectations() {
            {
                lakeService.unlockTabletMetadata((UnlockTabletMetadataRequest) any);
                result = CompletableFuture.completedFuture(new UnlockTabletMetadataResponse());
            }
        };

        Future<UnlockTabletMetadataResponse> result =
                lakeServiceWithMetrics.unlockTabletMetadata(new UnlockTabletMetadataRequest());
        assertNotNull(result);
    }

    @Test
    public void testUploadSnapshots() throws Exception {
        new Expectations() {
            {
                lakeService.uploadSnapshots((UploadSnapshotsRequest) any);
                result = CompletableFuture.completedFuture(new UploadSnapshotsResponse());
            }
        };

        Future<UploadSnapshotsResponse> result = lakeServiceWithMetrics.uploadSnapshots(new UploadSnapshotsRequest());
        assertNotNull(result);
    }

    @Test
    public void testRestoreSnapshots() throws Exception {
        new Expectations() {
            {
                lakeService.restoreSnapshots((RestoreSnapshotsRequest) any);
                result = CompletableFuture.completedFuture(new RestoreSnapshotsResponse());
            }
        };

        Future<RestoreSnapshotsResponse> result = lakeServiceWithMetrics.restoreSnapshots(new RestoreSnapshotsRequest());
        assertNotNull(result);
    }

    @Test
    public void testAbortCompaction() throws Exception {
        new Expectations() {
            {
                lakeService.abortCompaction((AbortCompactionRequest) any);
                result = CompletableFuture.completedFuture(new AbortCompactionResponse());
            }
        };

        Future<AbortCompactionResponse> result = lakeServiceWithMetrics.abortCompaction(new AbortCompactionRequest());
        assertNotNull(result);
    }

    @Test
    public void testVacuum() throws Exception {
        new Expectations() {
            {
                lakeService.vacuum((VacuumRequest) any);
                result = CompletableFuture.completedFuture(new VacuumResponse());
            }
        };

        Future<VacuumResponse> result = lakeServiceWithMetrics.vacuum(new VacuumRequest());
        assertNotNull(result);
    }
}
