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

package com.starrocks.connector.paimon;

import com.starrocks.connector.index.ConnectorIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResult;

import java.util.Objects;

/**
 * Complete candidate row IDs bound to one table, branch and snapshot.
 *
 * <p>Paimon uses an explicit result as-is: FULL mode does not add uncovered row ranges. Producers
 * must include those ranges or prove full coverage. This scalar executor requires full coverage
 * and only supplies row IDs to data-evolution snapshot scans, never primary-key or incremental scans.
 */
public final class PaimonGlobalIndexResult implements ConnectorIndexResult {
    private final String tablePath;
    private final String branch;
    private final long snapshotId;
    private final GlobalIndexResult result;

    public PaimonGlobalIndexResult(String tablePath, String branch, long snapshotId, GlobalIndexResult result) {
        if (snapshotId < 0) {
            throw new IllegalArgumentException("Paimon index result requires a valid snapshot");
        }
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath is null");
        this.branch = Objects.requireNonNull(branch, "branch is null");
        this.snapshotId = snapshotId;
        this.result = Objects.requireNonNull(result, "result is null");
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getBranch() {
        return branch;
    }

    @Override
    public long getSnapshotId() {
        return snapshotId;
    }

    public GlobalIndexResult getResult() {
        return result;
    }
}
