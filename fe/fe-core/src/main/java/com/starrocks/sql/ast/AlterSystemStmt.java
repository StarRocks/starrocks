// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;

public class AlterSystemStmt extends DdlStmt {
    private final AlterClause alterClause;

    public AlterSystemStmt(AlterClause alterClause) {
        this.alterClause = alterClause;
    }

    public AlterClause getAlterClause() {
        return alterClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterSystemStmt(this, context);
    }

    public boolean isSupportNewPlanner() {
        return true;
    }
}
