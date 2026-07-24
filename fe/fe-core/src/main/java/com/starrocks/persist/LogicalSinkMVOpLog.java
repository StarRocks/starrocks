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
import com.starrocks.catalog.LogicalSinkMVMeta;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

/**
 * Journal entry for creating/dropping a CK-compatible logical sink MV
 * ({@code CREATE MATERIALIZED VIEW ... TO <table>}). Persisted via Gson.
 *
 * <p>For create, {@link #meta} carries the full binding. For drop, only {@link #dbId},
 * {@link #baseTableId} and {@link #mvId} are meaningful.
 */
public class LogicalSinkMVOpLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "baseTableId")
    private long baseTableId;

    // Present for create.
    @SerializedName(value = "meta")
    private LogicalSinkMVMeta meta;

    // Present for drop.
    @SerializedName(value = "mvId")
    private long mvId;

    public LogicalSinkMVOpLog() {
    }

    public static LogicalSinkMVOpLog forCreate(long dbId, long baseTableId, LogicalSinkMVMeta meta) {
        LogicalSinkMVOpLog log = new LogicalSinkMVOpLog();
        log.dbId = dbId;
        log.baseTableId = baseTableId;
        log.meta = meta;
        log.mvId = meta.getMvId();
        return log;
    }

    public static LogicalSinkMVOpLog forDrop(long dbId, long baseTableId, long mvId) {
        LogicalSinkMVOpLog log = new LogicalSinkMVOpLog();
        log.dbId = dbId;
        log.baseTableId = baseTableId;
        log.mvId = mvId;
        return log;
    }

    public long getDbId() {
        return dbId;
    }

    public long getBaseTableId() {
        return baseTableId;
    }

    public LogicalSinkMVMeta getMeta() {
        return meta;
    }

    public long getMvId() {
        return mvId;
    }

    public static LogicalSinkMVOpLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LogicalSinkMVOpLog.class);
    }
}
