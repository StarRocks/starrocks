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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

// CK-compatible `CREATE MATERIALIZED VIEW ... TO <target>` logical-sink MV metadata.
// Registered on the base OlapTable. Describes a transform pipeline that fans base-table loads into a
// pre-existing target table (no independent storage / rollup index). Persistence wired in a later slice.
public class LogicalSinkMV {
    @SerializedName("mvId")
    private long mvId;
    @SerializedName("mvName")
    private String mvName;
    @SerializedName("baseTableId")
    private long baseTableId;
    @SerializedName("targetTableId")
    private long targetTableId;
    @SerializedName("defineSql")
    private String defineSql;

    public LogicalSinkMV(long mvId, String mvName, long baseTableId, long targetTableId, String defineSql) {
        this.mvId = mvId;
        this.mvName = mvName;
        this.baseTableId = baseTableId;
        this.targetTableId = targetTableId;
        this.defineSql = defineSql;
    }

    public long getMvId() {
        return mvId;
    }

    public String getMvName() {
        return mvName;
    }

    public long getBaseTableId() {
        return baseTableId;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public String getDefineSql() {
        return defineSql;
    }
}
