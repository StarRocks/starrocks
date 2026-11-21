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

package com.starrocks.connector.fluss;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.client.metadata.AcquireKvSnapshotLeaseResult;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.TableBucket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FlussKvSnapshotLeaseTest {
    private static final Map<TableBucket, Long> SNAPSHOTS = Map.of(new TableBucket(42L, 0), 5L);

    @Mocked
    private Admin admin;

    @Mocked
    private KvSnapshotLease kvSnapshotLease;

    @Test
    public void testLeaseIsDroppedOnceHoweverOftenItIsClosed() throws Exception {
        new Expectations() {
            {
                admin.createKvSnapshotLease("starrocks-q1", 60_000L);
                result = kvSnapshotLease;
                kvSnapshotLease.acquireSnapshots(SNAPSHOTS);
                result = CompletableFuture.completedFuture(new AcquireKvSnapshotLeaseResult(Map.of()));
                kvSnapshotLease.dropLease();
                result = CompletableFuture.failedFuture(new RuntimeException("coordinator unavailable"));
            }
        };
        FlussKvSnapshotLease lease = new FlussKvSnapshotLease(admin, "starrocks-q1", 60_000L);

        Assertions.assertTrue(lease.acquire(SNAPSHOTS).isEmpty());
        Assertions.assertEquals("starrocks-q1", lease.leaseId().orElseThrow());
        // A failed drop is only logged: the lease still expires on its own.
        lease.close();
        lease.close();
        new Verifications() {
            {
                kvSnapshotLease.dropLease();
                times = 1;
            }
        };
    }

    @Test
    public void testNothingIsCreatedInFlussUntilASnapshotIsLeased() throws Exception {
        FlussKvSnapshotLease lease = new FlussKvSnapshotLease(admin, "starrocks-q1", 60_000L);

        Assertions.assertTrue(lease.acquire(Map.of()).isEmpty());
        Assertions.assertTrue(FlussKvSnapshotLease.NONE.acquire(SNAPSHOTS).isEmpty());
        lease.close();
        FlussKvSnapshotLease.NONE.close();
        Assertions.assertTrue(lease.leaseId().isEmpty());
        new Verifications() {
            {
                admin.createKvSnapshotLease(anyString, anyLong);
                times = 0;
            }
        };
    }

    @Test
    public void testServerWithoutLeaseApiReadsWithoutALease() throws Exception {
        new Expectations() {
            {
                admin.createKvSnapshotLease(anyString, anyLong);
                result = kvSnapshotLease;
                kvSnapshotLease.acquireSnapshots(SNAPSHOTS);
                result = CompletableFuture.failedFuture(new UnsupportedVersionException("no lease API"));
            }
        };
        FlussKvSnapshotLease lease = new FlussKvSnapshotLease(admin, "starrocks-q1", 60_000L);

        Assertions.assertTrue(lease.acquire(SNAPSHOTS).isEmpty());
        Assertions.assertTrue(lease.leaseId().isEmpty());
        lease.close();
        new Verifications() {
            {
                kvSnapshotLease.dropLease();
                times = 0;
            }
        };
    }
}
