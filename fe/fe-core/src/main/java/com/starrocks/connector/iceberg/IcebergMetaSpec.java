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

import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.SerializedMetaSpec;

import java.util.List;

public class IcebergMetaSpec implements SerializedMetaSpec {
    public static final IcebergMetaSpec EMPTY = new IcebergMetaSpec(null, null, false);

    private final String serializedTable;
    private final List<RemoteMetaSplit> splits;
    private boolean loadColumnStats;

    public IcebergMetaSpec(String serializedTable, List<RemoteMetaSplit> splits, boolean loadColumnStats) {
        this.serializedTable = serializedTable;
        this.splits = splits;
        this.loadColumnStats = loadColumnStats;
    }


    @Override
    public String getTable() {
        return serializedTable;
    }

    @Override
    public List<RemoteMetaSplit> getSplits() {
        return splits;
    }

    public boolean loadColumnStats() {
        return loadColumnStats;
    }

    public void setLoadColumnStats(boolean loadColumnStats) {
        this.loadColumnStats = loadColumnStats;
    }
}
