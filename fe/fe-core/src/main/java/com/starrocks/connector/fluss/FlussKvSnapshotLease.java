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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The KV snapshot lease of one Fluss scan. Fluss keeps the leased snapshots until the lease is
 * dropped or expires, as for Fluss's own Flink source. The lease is created in Fluss on the first
 * acquire, and {@link #close()} drops it once, without waiting.
 */
public final class FlussKvSnapshotLease implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(FlussKvSnapshotLease.class);

    /** Leases nothing: for reads that need no KV snapshot, and for a plain EXPLAIN. */
    public static final FlussKvSnapshotLease NONE = new FlussKvSnapshotLease(null, null, 0);

    private final Admin admin;
    private final String leaseId;
    private final long leaseDurationMs;
    // Written while planning, read again by the thread that clears the scan.
    private volatile KvSnapshotLease lease;
    private volatile boolean unsupported;
    private final AtomicBoolean dropped = new AtomicBoolean();

    FlussKvSnapshotLease(Admin admin, String leaseId, long leaseDurationMs) {
        this.admin = admin;
        this.leaseId = leaseId;
        this.leaseDurationMs = leaseDurationMs;
    }

    /**
     * Leases the given KV snapshots and returns the buckets whose snapshot Fluss has already cleaned up.
     * A Fluss server without the lease API leases nothing, so the scan reads without a lease.
     */
    Set<TableBucket> acquire(Map<TableBucket, Long> snapshotIds) throws Exception {
        if (this == NONE || unsupported || snapshotIds.isEmpty()) {
            return Set.of();
        }
        if (lease == null) {
            lease = admin.createKvSnapshotLease(leaseId, leaseDurationMs);
        }
        try {
            return lease.acquireSnapshots(snapshotIds).get().getUnavailableTableBucketSet();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class).isEmpty()) {
                throw e;
            }
            LOG.warn("Fluss server does not support KV snapshot leases; reading without lease {}, " +
                    "so Fluss may clean up KV snapshots mid-scan.", leaseId);
            unsupported = true;
            return Set.of();
        }
    }

    /** The id of the lease held in Fluss, if any. */
    public Optional<String> leaseId() {
        return lease == null || unsupported ? Optional.empty() : Optional.of(leaseId);
    }

    @Override
    public void close() {
        KvSnapshotLease held = lease;
        if (held == null || unsupported || !dropped.compareAndSet(false, true)) {
            return;
        }
        held.dropLease().whenComplete((ignored, e) -> {
            if (e != null) {
                LOG.warn("Failed to drop Fluss KV snapshot lease {}; it expires after {} ms.",
                        leaseId, leaseDurationMs, e);
            }
        });
    }
}
