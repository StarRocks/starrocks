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
import com.starrocks.statistic.expression.ExpressionStatsKey;
import com.starrocks.statistic.expression.ExpressionStatsKind;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class ExpressionStatsMeta {
    @SerializedName("key")
    private ExpressionStatsKey key;

    @SerializedName("kind")
    private ExpressionStatsKind kind;

    @SerializedName("name")
    private String name;

    @SerializedName("expressionSql")
    private String expressionSql;

    @SerializedName("baseColumnIds")
    private List<Integer> baseColumnIds;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    // For Gson.
    public ExpressionStatsMeta() {
    }

    public ExpressionStatsMeta(ExpressionStatsKey key, ExpressionStatsKind kind, String name, String expressionSql,
                               List<Integer> baseColumnIds, StatsConstants.AnalyzeType type,
                               LocalDateTime updateTime) {
        this.key = key;
        this.kind = kind;
        this.name = name;
        this.expressionSql = expressionSql;
        this.baseColumnIds = baseColumnIds;
        this.type = type;
        this.updateTime = updateTime;
    }

    public ExpressionStatsKey getKey() {
        return key;
    }

    public ExpressionStatsKind getKind() {
        return kind;
    }

    public String getName() {
        return name;
    }

    public String getExpressionSql() {
        return expressionSql;
    }

    public List<Integer> getBaseColumnIds() {
        return baseColumnIds;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public String getStorageColumnName() {
        return key.getStorageColumnName();
    }

    public String simpleString() {
        return String.format("(%s,%s,%s)", key.getNormalizedExpression(), kind, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExpressionStatsMeta)) {
            return false;
        }
        ExpressionStatsMeta that = (ExpressionStatsMeta) o;
        return Objects.equals(key, that.key) && kind == that.kind && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, kind, type);
    }
}
