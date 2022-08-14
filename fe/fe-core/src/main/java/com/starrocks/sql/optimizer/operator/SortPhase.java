// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator;

public enum SortPhase {
    PARTIAL,
    FINAL;

    public boolean isPartial() {
        return this.equals(SortPhase.PARTIAL);
    }

    public boolean isFinal() {
        return this.equals(SortPhase.FINAL);
    }
}
