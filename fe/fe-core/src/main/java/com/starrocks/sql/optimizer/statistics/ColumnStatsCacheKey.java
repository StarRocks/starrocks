// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.statistics;

import java.util.Objects;

class ColumnStatsCacheKey {
    public final long tableId;
    public final String column;

    public ColumnStatsCacheKey(long tableId, String column) {
        this.tableId = tableId;
        this.column = column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnStatsCacheKey cacheKey = (ColumnStatsCacheKey) o;

        if (tableId != cacheKey.tableId) {
            return false;
        }
        return column.equalsIgnoreCase(cacheKey.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, column);
    }
}