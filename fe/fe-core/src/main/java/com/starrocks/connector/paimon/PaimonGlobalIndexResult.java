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

/** A snapshot-bound Paimon row-id result that can be converted into {@code IndexedSplit}s. */
public final class PaimonGlobalIndexResult implements ConnectorIndexResult {
    private final long snapshotId;
    private final GlobalIndexResult result;

    public PaimonGlobalIndexResult(long snapshotId, GlobalIndexResult result) {
        if (snapshotId < 0) {
            throw new IllegalArgumentException("Paimon global index result requires a valid snapshot id");
        }
        this.snapshotId = snapshotId;
        this.result = Objects.requireNonNull(result, "result is null");
    }

    @Override
    public long getSnapshotId() {
        return snapshotId;
    }

    public GlobalIndexResult getResult() {
        return result;
    }
}
