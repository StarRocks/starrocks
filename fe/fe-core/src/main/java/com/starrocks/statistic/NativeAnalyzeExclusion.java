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

package com.starrocks.statistic;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
public class NativeAnalyzeExclusion implements AnalyzeExclusion, Writable {
    @SerializedName("id")
    private long id;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("addTime")
    private LocalDateTime addTime;
    @SerializedName("type")
    private AnalyzeType type;
    public NativeAnalyzeExclusion(long dbId, long tableId, AnalyzeType type) {
        this.id = -1;
        this.dbId = dbId;
        this.tableId = tableId;
        this.addTime = LocalDateTime.now();
        this.type = type;
    }
    @Override
    public long getId() {
        return id;
    }
    @Override
    public boolean isNative() {
        return true;
    }
    @Override
    public String getCatalogName() {
        return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    }
    @Override
    public String getDbName() throws MetaNotFoundException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        return db.getOriginName();
    }
    @Override
    public String getTableName() throws MetaNotFoundException {
        return StatisticUtils.getTable(this.dbId, this.tableId).getName();
    }
    @Override
    public boolean isAllDbExclusion() {
        return dbId == StatsConstants.DEFAULT_ALL_ID;
    }
    @Override
    public boolean isAllTableExclusion() {
        return tableId == StatsConstants.DEFAULT_ALL_ID;
    }
    @Override
    public LocalDateTime getAddTime() {
        return addTime;
    }
    @Override
    public void setId(long id) {
        this.id = id;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }
    public static NativeAnalyzeExclusion read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, NativeAnalyzeExclusion.class);
    }
    @Override
    public String toString() {
        return "";
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NativeAnalyzeExclusion that = (NativeAnalyzeExclusion) obj;
        return Objects.equal(dbId, that.dbId) &&
                Objects.equal(tableId, that.tableId) && Objects.equal(type, that.type);
    }
}
