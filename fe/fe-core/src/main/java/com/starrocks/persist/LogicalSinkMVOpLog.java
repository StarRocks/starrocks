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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

// Edit-log payload for CK-compatible logical-sink MV create/drop. gson-serialized via logJsonObject
// (Writable has a default no-op write()), so this is a marker + gson-annotated fields only.
public class LogicalSinkMVOpLog implements Writable {
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("baseTableId")
    private long baseTableId;
    @SerializedName("mvId")
    private long mvId;
    @SerializedName("mvName")
    private String mvName;
    @SerializedName("targetTableId")
    private long targetTableId;
    @SerializedName("defineSql")
    private String defineSql;

    public LogicalSinkMVOpLog(long dbId, long baseTableId, long mvId, String mvName, long targetTableId,
                              String defineSql) {
        this.dbId = dbId;
        this.baseTableId = baseTableId;
        this.mvId = mvId;
        this.mvName = mvName;
        this.targetTableId = targetTableId;
        this.defineSql = defineSql;
    }

    public LogicalSinkMVOpLog(long dbId, long baseTableId, long mvId) {
        this(dbId, baseTableId, mvId, null, 0L, null);
    }

    public long getDbId() {
        return dbId;
    }

    public long getBaseTableId() {
        return baseTableId;
    }

    public long getMvId() {
        return mvId;
    }

    public String getMvName() {
        return mvName;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public String getDefineSql() {
        return defineSql;
    }
}
