// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;

public class UnitIdentifier implements ParseNode {
    private final String description;

    public UnitIdentifier(String description) {
        this.description = description.toUpperCase();
    }

    public String getDescription() {
        return description;
    }
}
