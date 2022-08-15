// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
