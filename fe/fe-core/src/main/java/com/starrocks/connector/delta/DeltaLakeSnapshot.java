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

package com.starrocks.connector.delta;

import io.delta.kernel.internal.SnapshotImpl;

public class DeltaLakeSnapshot {
    private final String dbName;
    private final String tableName;
    private final DeltaLakeEngine deltaLakeEngine;
    private final SnapshotImpl snapshot;
    private final long createTime;
    private final String path;

    public DeltaLakeSnapshot(String dbName, String tableName, DeltaLakeEngine engine, SnapshotImpl snapshot,
                             long createTime, String path) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.deltaLakeEngine = engine;
        this.snapshot = snapshot;
        this.createTime = createTime;
        this.path = path;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public DeltaLakeEngine getDeltaLakeEngine() {
        return deltaLakeEngine;
    }

    public SnapshotImpl getSnapshot() {
        return snapshot;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getPath() {
        return path;
    }
}
