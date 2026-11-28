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

import java.util.List;

/** Index planning facts for one final Paimon data scan. */
public record PaimonGlobalIndexPlanInfo(PaimonGlobalIndexMode mode, Result result, Reason reason, Details details) {
    public enum Result {
        APPLIED("distributed"),
        FALLBACK("ordinary"),
        SKIPPED("ordinary"),
        SDK_MANAGED("sdk_local"),
        DISABLED("ordinary");

        private final String path;

        Result(String path) {
            this.path = path;
        }

        public String path() {
            return path;
        }
    }

    public enum Reason {
        TABLE_INDEX_DISABLED,
        UNSUPPORTED_SCAN_SCOPE,
        NO_INDEX_REQUEST,
        INDEX_EVALUATION_FAILED,
        INDEXED_SPLIT_PLANNING_FAILED
    }

    public record Details(String indexType, List<String> indexedColumns, long snapshotId,
                          int shardCount, long candidateRowIds) {
        public Details {
            indexedColumns = List.copyOf(indexedColumns);
        }
    }
}
