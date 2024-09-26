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

import com.google.gson.annotations.SerializedName;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Meta of column-level statistics
 */
public class ColumnStatsMeta {

    @SerializedName("columnName")
    private String columnName;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    public ColumnStatsMeta(String columnName, StatsConstants.AnalyzeType type, LocalDateTime updateTime) {
        this.columnName = columnName;
        this.type = type;
        this.updateTime = updateTime;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public void setType(StatsConstants.AnalyzeType type) {
        this.type = type;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public String simpleString() {
        return String.format("(%s,%s)", columnName, type.toString());
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ColumnStatsMeta{");
        sb.append("columnName='").append(columnName).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnStatsMeta that = (ColumnStatsMeta) o;
        return Objects.equals(columnName, that.columnName) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, type);
    }
}
