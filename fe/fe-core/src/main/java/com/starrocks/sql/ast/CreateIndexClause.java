// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Index;

public class CreateIndexClause extends AlterTableClause {
    // index definition class
    private final IndexDef indexDef;
    // index internal class
    private Index index;

    public CreateIndexClause(TableName tableName, IndexDef indexDef, boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.indexDef = indexDef;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public IndexDef getIndexDef() {
        return indexDef;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIndexClause(this, context);
    }
}
