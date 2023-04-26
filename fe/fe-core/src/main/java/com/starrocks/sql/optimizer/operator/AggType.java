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


package com.starrocks.sql.optimizer.operator;

/**
 * The default agg type is GLOBAL.
 * <p>
 * For normal agg query, we use distributed one stage agg:
 * LOCAL agg -> GLOBAL agg
 * <p>
 * For agg query with one distinct and group by column:
 * LOCAL agg -> DISTINCT_GLOBAL agg -> GLOBAL agg
 * <p>
 * For agg query with only one distinct and no group by column:
 * <p>
 * LOCAL agg -> DISTINCT_GLOBAL agg -> DISTINCT_LOCAL agg -> GLOBAL agg
 */
public enum AggType {
    LOCAL,
    GLOBAL,
    DISTINCT_GLOBAL,
    DISTINCT_LOCAL;

    public boolean isLocal() {
        return this.equals(AggType.LOCAL);
    }

    public boolean isGlobal() {
        return this.equals(AggType.GLOBAL);
    }

    public boolean isDistinctGlobal() {
        return this.equals(AggType.DISTINCT_GLOBAL);
    }

    public boolean isDistinct() {
        return this.equals(AggType.DISTINCT_LOCAL) || this.equals(AggType.DISTINCT_GLOBAL);
    }

    public boolean isDistinctLocal() {
        return this.equals(AggType.DISTINCT_LOCAL);
    }
}
