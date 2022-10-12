// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import java.util.Objects;

/**
 * Currently, the column doesn't have global unique id,
 * so we use table id + column name to identify one column
 */
public final class ColumnIdentifier {
    private final long tableId;
    private final String columnName;

    private long dbId = -1;

    public ColumnIdentifier(long tableId, String columnName) {
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public ColumnIdentifier(long dbId, long tableId, String columnName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnIdentifier)) {
            return false;
        }

        ColumnIdentifier columnIdentifier = (ColumnIdentifier) o;
        if (this == columnIdentifier) {
            return true;
        }
        return tableId == columnIdentifier.tableId && columnName.equalsIgnoreCase(columnIdentifier.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, columnName);
    }
}