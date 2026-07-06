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

package com.starrocks.connector.iceberg.procedure;

import com.google.common.collect.Iterables;
import com.google.gson.JsonObject;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.IcebergUtil;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Mutable execution statistics of a single iceberg metadata maintenance procedure run
 * (expire_snapshots / remove_orphan_files / rewrite_manifests). The procedure fills
 * input-side fields and sets {@code executed} once it has committed work; the caller
 * sets {@code committed} after the surrounding transaction is published and then fills
 * output-side fields via {@link #collectOutputs(Table)}.
 *
 * Field default convention: expire/rewrite fields are measurements — -1 means
 * "not collected yet" (e.g. output side before the commit) and is skipped by
 * {@link #toJson()}; the remove_orphan_files fields are accumulators that count from
 * the start of the scan, so 0 is a valid value (scanned, found nothing) and they are
 * accurate at any point in time, even after a mid-run failure.
 */
public class IcebergMaintenanceTaskStats {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceTaskStats.class);

    // an output manifest is considered a residual small file when it is smaller
    // than a quarter of the manifest target size
    private static final long SMALL_MANIFEST_TARGET_SIZE_DIVISOR = 4;

    private IcebergTableOperation operation;
    // true only when the procedure actually committed work into the transaction
    // (or, for remove_orphan_files, finished its scan-and-delete pass)
    private boolean executed = false;
    // true only after the surrounding transaction was successfully published via
    // commitTransaction(); gates expire/rewrite effect metrics, because a procedure
    // may have staged work (executed=true) whose publication later fails
    private boolean committed = false;

    // expire_snapshots
    private long snapshotCountInput = -1;
    private long snapshotCountOutput = -1;

    // rewrite_manifests
    private long manifestCountInput = -1;
    private long manifestBytesInput = -1;
    private long manifestCountOutput = -1;
    private long manifestBytesOutput = -1;
    private long manifestSmallFilesOutput = -1;
    private long manifestTargetSizeBytes = -1;

    // remove_orphan_files
    private long orphanFilesDetected = 0;
    private long orphanFilesRemoved = 0;
    private long orphanBytesRemoved = 0;
    // some deletions succeeded before a failure (orphan files are deleted in batches)
    private boolean partiallyApplied = false;

    public IcebergTableOperation getOperation() {
        return operation;
    }

    public void setOperation(IcebergTableOperation operation) {
        this.operation = operation;
    }

    public boolean isExecuted() {
        return executed;
    }

    public void setExecuted(boolean executed) {
        this.executed = executed;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public long getSnapshotCountInput() {
        return snapshotCountInput;
    }

    public void setSnapshotCountInput(long snapshotCountInput) {
        this.snapshotCountInput = snapshotCountInput;
    }

    public long getSnapshotCountOutput() {
        return snapshotCountOutput;
    }

    public long getManifestCountInput() {
        return manifestCountInput;
    }

    public void setManifestCountInput(long manifestCountInput) {
        this.manifestCountInput = manifestCountInput;
    }

    public long getManifestBytesInput() {
        return manifestBytesInput;
    }

    public void setManifestBytesInput(long manifestBytesInput) {
        this.manifestBytesInput = manifestBytesInput;
    }

    public long getManifestCountOutput() {
        return manifestCountOutput;
    }

    public long getManifestBytesOutput() {
        return manifestBytesOutput;
    }

    public long getManifestSmallFilesOutput() {
        return manifestSmallFilesOutput;
    }

    public long getManifestTargetSizeBytes() {
        return manifestTargetSizeBytes;
    }

    public void setManifestTargetSizeBytes(long manifestTargetSizeBytes) {
        this.manifestTargetSizeBytes = manifestTargetSizeBytes;
    }

    public long getOrphanFilesDetected() {
        return orphanFilesDetected;
    }

    public void addOrphanDetected(long files) {
        this.orphanFilesDetected += files;
    }

    public long getOrphanFilesRemoved() {
        return orphanFilesRemoved;
    }

    public long getOrphanBytesRemoved() {
        return orphanBytesRemoved;
    }

    public void addOrphanRemoved(long files, long bytes) {
        this.orphanFilesRemoved += files;
        this.orphanBytesRemoved += bytes;
    }

    public boolean isPartiallyApplied() {
        return partiallyApplied;
    }

    public void setPartiallyApplied(boolean partiallyApplied) {
        this.partiallyApplied = partiallyApplied;
    }

    /**
     * Whether this run actually changed table state. Only meaningful on the success
     * path (after {@link #collectOutputs(Table)} has filled the output side). Used to
     * classify a successful run as {@code success} (changed something) versus
     * {@code skipped} (ran but had nothing to do, e.g. an expire that removed no
     * snapshots).
     */
    public boolean hasMaterialChange() {
        if (operation == null) {
            return false;
        }
        switch (operation) {
            case EXPIRE_SNAPSHOTS:
                return snapshotCountInput >= 0 && snapshotCountOutput >= 0
                        && snapshotCountOutput < snapshotCountInput;
            case REWRITE_MANIFESTS:
                // rewrite_manifests only stages and commits work when it actually
                // rewrites; a no-op early-returns with executed == false
                return executed;
            case REMOVE_ORPHAN_FILES:
                return orphanFilesRemoved > 0;
            default:
                return false;
        }
    }

    /**
     * Fill output-side fields from a refreshed table. Must be called AFTER the
     * transaction has been committed and the table refreshed.
     */
    public void collectOutputs(Table refreshedTable) {
        if (operation == null) {
            return;
        }
        switch (operation) {
            case EXPIRE_SNAPSHOTS:
                snapshotCountOutput = Iterables.size(refreshedTable.snapshots());
                break;
            case REWRITE_MANIFESTS:
                if (refreshedTable.currentSnapshot() == null) {
                    break;
                }
                long count = 0;
                long totalBytes = 0;
                long smallFiles = 0;
                long smallThresholdBytes = manifestTargetSizeBytes > 0
                        ? manifestTargetSizeBytes / SMALL_MANIFEST_TARGET_SIZE_DIVISOR : -1;
                try (CloseableIterable<ManifestFile> manifests =
                        IcebergUtil.readManifests(refreshedTable.currentSnapshot(), refreshedTable.io())) {
                    for (ManifestFile manifest : manifests) {
                        count++;
                        totalBytes += manifest.length();
                        if (smallThresholdBytes > 0 && manifest.length() < smallThresholdBytes) {
                            smallFiles++;
                        }
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to read manifests for rewrite_manifests output stats", e);
                    break;
                }
                manifestCountOutput = count;
                manifestBytesOutput = totalBytes;
                if (smallThresholdBytes > 0) {
                    manifestSmallFilesOutput = smallFiles;
                }
                break;
            default:
                // remove_orphan_files: outputs are accumulated during execute()
                break;
        }
    }

    /**
     * Serialize per-action details as a JSON object containing only the keys
     * relevant to this operation, for the iceberg_maintenance_tasks system table.
     */
    public String toJson() {
        JsonObject json = new JsonObject();
        if (operation == null) {
            return json.toString();
        }
        switch (operation) {
            case EXPIRE_SNAPSHOTS:
                addIfSet(json, "snapshot_count_input", snapshotCountInput);
                addIfSet(json, "snapshot_count_output", snapshotCountOutput);
                if (snapshotCountInput >= 0 && snapshotCountOutput >= 0) {
                    json.addProperty("snapshot_removed_count", snapshotCountInput - snapshotCountOutput);
                }
                break;
            case REWRITE_MANIFESTS:
                addIfSet(json, "manifest_file_count_input", manifestCountInput);
                addIfSet(json, "manifest_bytes_total_input", manifestBytesInput);
                addIfSet(json, "manifest_file_count_output", manifestCountOutput);
                addIfSet(json, "manifest_bytes_total_output", manifestBytesOutput);
                addIfSet(json, "small_manifest_files_count_output", manifestSmallFilesOutput);
                break;
            case REMOVE_ORPHAN_FILES:
                // accumulators are valid at any point (0 = scanned, found nothing),
                // so they are emitted unconditionally — see the class javadoc
                json.addProperty("orphan_file_count_detected", orphanFilesDetected);
                json.addProperty("orphan_file_removed_count", orphanFilesRemoved);
                json.addProperty("orphan_bytes_removed", orphanBytesRemoved);
                break;
            default:
                break;
        }
        return json.toString();
    }

    private static void addIfSet(JsonObject json, String key, long value) {
        if (value >= 0) {
            json.addProperty(key, value);
        }
    }
}
