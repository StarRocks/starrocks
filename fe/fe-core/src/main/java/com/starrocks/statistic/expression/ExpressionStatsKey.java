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

package com.starrocks.statistic.expression;

import com.google.common.hash.Hashing;
import com.google.gson.annotations.SerializedName;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ExpressionStatsKey {
    public static final String STORAGE_PREFIX = "__sr_expr_";

    @SerializedName("normalizedExpression")
    private String normalizedExpression;

    @SerializedName("storageColumnName")
    private String storageColumnName;

    // For Gson.
    public ExpressionStatsKey() {
    }

    private ExpressionStatsKey(String normalizedExpression, String storageColumnName) {
        this.normalizedExpression = normalizedExpression;
        this.storageColumnName = storageColumnName;
    }

    public static ExpressionStatsKey create(String normalizedExpression) {
        String digest = Hashing.sha256().hashString(normalizedExpression, StandardCharsets.UTF_8).toString();
        return new ExpressionStatsKey(normalizedExpression, STORAGE_PREFIX + digest.substring(0, 32));
    }

    public String getNormalizedExpression() {
        return normalizedExpression;
    }

    public String getStorageColumnName() {
        return storageColumnName;
    }

    @Override
    public String toString() {
        return normalizedExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExpressionStatsKey)) {
            return false;
        }
        ExpressionStatsKey that = (ExpressionStatsKey) o;
        return Objects.equals(normalizedExpression, that.normalizedExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(normalizedExpression);
    }
}
