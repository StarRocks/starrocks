// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.TableName;

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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropIndexClause(this, context);
    }
}
