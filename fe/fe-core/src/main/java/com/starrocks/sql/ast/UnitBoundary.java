// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;

/*
 * UnitBoundary used to specify time boundary of time_slice:
 * FLOOR specify START as result time.
 * CEIL specify END as result time.
 */
public class UnitBoundary implements ParseNode {
    private final String description;

    public UnitBoundary(String description) {
        this.description = description.toUpperCase();
    }

    public String getDescription() {
        return description;
    }
}
