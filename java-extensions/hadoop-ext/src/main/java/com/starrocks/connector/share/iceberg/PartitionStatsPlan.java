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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;

import java.util.Collections;
import java.util.List;

// Result of IcebergPartitionStatsLookup.plan(...). Tells the caller how to satisfy a
// "partitions at snapshot X" request via a partition statistics file:
//   NONE         — fall back to the per-manifest scan (reason() explains why);
//   BASE         — read statsFile() directly, it matches targetSnapshotId();
//   INCREMENTAL  — read statsFile() and append in-memory the manifests from
//                  incrementalManifests() (those added strictly between the stats snapshot and
//                  targetSnapshotId()).
public final class PartitionStatsPlan {
    public enum Mode { NONE, BASE, INCREMENTAL }

    public static final String REASON_DISABLED_BY_SESSION = "disabled_by_session";
    public static final String REASON_NO_CURRENT_SNAPSHOT = "no_current_snapshot";
    public static final String REASON_MISSING_TARGET_SNAPSHOT = "missing_target_snapshot";
    public static final String REASON_NO_STATS_FILE = "no_stats_file";
    public static final String REASON_MISSING_SNAPSHOT_LINEAGE = "missing_snapshot_lineage";

    private final Mode mode;
    private final PartitionStatisticsFile statsFile;
    private final long targetSnapshotId;
    private final List<ManifestFile> incrementalManifests;
    private final String reason;

    private PartitionStatsPlan(Mode mode,
                               PartitionStatisticsFile statsFile,
                               long targetSnapshotId,
                               List<ManifestFile> incrementalManifests,
                               String reason) {
        this.mode = mode;
        this.statsFile = statsFile;
        this.targetSnapshotId = targetSnapshotId;
        this.incrementalManifests = incrementalManifests;
        this.reason = reason;
    }

    public static PartitionStatsPlan none(String reason) {
        return new PartitionStatsPlan(Mode.NONE, null, -1, Collections.emptyList(), reason);
    }

    public static PartitionStatsPlan base(PartitionStatisticsFile statsFile, long targetSnapshotId) {
        return new PartitionStatsPlan(Mode.BASE, statsFile, targetSnapshotId, Collections.emptyList(), null);
    }

    public static PartitionStatsPlan incremental(PartitionStatisticsFile statsFile,
                                                 long targetSnapshotId,
                                                 List<ManifestFile> incrementalManifests) {
        return new PartitionStatsPlan(Mode.INCREMENTAL, statsFile, targetSnapshotId, incrementalManifests, null);
    }

    public Mode mode() {
        return mode;
    }

    public PartitionStatisticsFile statsFile() {
        return statsFile;
    }

    public long targetSnapshotId() {
        return targetSnapshotId;
    }

    public List<ManifestFile> incrementalManifests() {
        return incrementalManifests;
    }

    public String reason() {
        return reason;
    }
}
