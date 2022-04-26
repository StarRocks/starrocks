package com.starrocks.sql.ast;// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.AlterTableClause;
import com.starrocks.analysis.TableName;

import java.util.Map;

public class DropIndexClause extends AlterTableClause {
    private final String indexName;

    public DropIndexClause(String indexName, TableName tableName, boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropIndexClause(this, context);
    }
}
