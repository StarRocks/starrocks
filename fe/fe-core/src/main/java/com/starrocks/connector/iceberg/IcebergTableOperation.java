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

package com.starrocks.connector.iceberg;

public enum IcebergTableOperation {
    EXPIRE_SNAPSHOTS,
    FAST_FORWARD,
    CHERRYPICK_SNAPSHOT,
    REMOVE_ORPHAN_FILES,
    ROLLBACK_TO_SNAPSHOT,
    REWRITE_DATA_FILES,
    UNKNOWN;

    
    public static IcebergTableOperation fromString(String opStr) {
        for (IcebergTableOperation op : IcebergTableOperation.values()) {
            if (op.name().equalsIgnoreCase(opStr)) {
                return op;
            }
        }
        return UNKNOWN;
    }
    
    public enum RewriteFileOption {
        REWRITE_ALL,  // rewrite all the files under the specified partitions, ignore other param like min_file_size_bytes, default false
        MIN_FILE_SIZE_BYTES, // to filter data file by size, default 256MB
        BATCH_SIZE, // the max size of total data files to rewrite at one time, default 10GB
        BATCH_PARALLELISM, // the parallelism between batches, default 1
        UNKNOWN;

        public static RewriteFileOption fromString(String catStr) {
            for (RewriteFileOption c : values()) {
                if (c.name().equalsIgnoreCase(catStr)) {
                    return c;
                }
            }
            return UNKNOWN;
        }
    }

}
