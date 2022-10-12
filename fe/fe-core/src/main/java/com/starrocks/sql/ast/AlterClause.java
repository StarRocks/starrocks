// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Maps;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ParseNode;

import java.util.Map;

// Alter clause.
public abstract class AlterClause implements ParseNode {

    protected AlterOpType opType;

    public AlterClause(AlterOpType opType) {
        this.opType = opType;
    }

    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

    public AlterOpType getOpType() {
        return opType;
    }

    public boolean isSupportNewPlanner() {
        return false;
    }
}
