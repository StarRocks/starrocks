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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.statistic.StatsConstants.AnalyzeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
public class ExternalAnalyzeExclusion implements AnalyzeExclusion, Writable {
    @SerializedName("id")
    private long id;
    @SerializedName("catalogName")
    private String catalogName;
    @SerializedName("dbName")
    private String dbName;
    @SerializedName("tableName")
    private String tableName;
    @SerializedName("addTime")
    private LocalDateTime addTime;
    @SerializedName("type")
    private AnalyzeType type;
    public ExternalAnalyzeExclusion(String catalogName, String dbName,
                                    String tableName, AnalyzeType type) {
        this.id = -1;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.addTime = LocalDateTime.now();
        this.type = type;
    }
    @Override
    public long getId() {
        return id;
    }
    @Override
    public boolean isNative() {
        return false;
    }
    @Override
    public String getCatalogName() {
        return catalogName;
    }
    @Override
    public String getDbName() {
        return dbName;
    }
    @Override
    public String getTableName() {
        return tableName;
    }
    @Override
    public boolean isAllDbExclusion() {
        return dbName == null;
    }
    @Override
    public boolean isAllTableExclusion() {
        return tableName == null;
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
    public static ExternalAnalyzeExclusion read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, ExternalAnalyzeExclusion.class);
    }
    public String toString() {
        return "";
    }
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ExternalAnalyzeExclusion that = (ExternalAnalyzeExclusion) obj;
        return Objects.equal(catalogName, that.catalogName)
                && Objects.equal(dbName, that.dbName)
                && Objects.equal(tableName, that.tableName)
                && Objects.equal(type, that.type);
    }
}
