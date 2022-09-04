// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

import java.util.Map;

// clause which is used to modify table properties
public class ModifyTablePropertiesClause extends AlterTableClause {
    private final Map<String, String> properties;

    public ModifyTablePropertiesClause(Map<String, String> properties) {
        super(AlterOpType.MODIFY_TABLE_PROPERTY);
        this.properties = properties;
    }

    public void setOpType(AlterOpType opType) {
        this.opType = opType;
    }

    public void setNeedTableStable(boolean needTableStable) {
        this.needTableStable = needTableStable;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyTablePropertiesClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
