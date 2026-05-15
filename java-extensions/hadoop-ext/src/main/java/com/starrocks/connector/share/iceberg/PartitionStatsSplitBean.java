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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PartitionStatsSplitBean implements Serializable {
    public static final String MODE_FULL = "full";
    public static final String MODE_BASE = "base";

    private String statsFilePath;
    private Long statsSnapshotId;
    private Long targetSnapshotId;
    private List<ManifestFileBean> incrementalManifests = new ArrayList<>();
    private String mode = MODE_FULL;

    public String getStatsFilePath() {
        return statsFilePath;
    }

    public void setStatsFilePath(String statsFilePath) {
        this.statsFilePath = statsFilePath;
    }

    public Long getStatsSnapshotId() {
        return statsSnapshotId;
    }

    public void setStatsSnapshotId(Long statsSnapshotId) {
        this.statsSnapshotId = statsSnapshotId;
    }

    public Long getTargetSnapshotId() {
        return targetSnapshotId;
    }

    public void setTargetSnapshotId(Long targetSnapshotId) {
        this.targetSnapshotId = targetSnapshotId;
    }

    public List<ManifestFileBean> getIncrementalManifests() {
        return incrementalManifests;
    }

    public void setIncrementalManifests(List<ManifestFileBean> incrementalManifests) {
        this.incrementalManifests = incrementalManifests;
    }

    public boolean hasIncrementalManifests() {
        return incrementalManifests != null && !incrementalManifests.isEmpty();
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public boolean isBaseMode() {
        return MODE_BASE.equalsIgnoreCase(mode);
    }

    public boolean isFullMode() {
        return mode == null || MODE_FULL.equalsIgnoreCase(mode);
    }
}
