// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class ApplyRowAccessPolicyClause extends AlterTableClause {
    private final WithRowAccessPolicy withRowAccessPolicy;

    public ApplyRowAccessPolicyClause(WithRowAccessPolicy withRowAccessPolicy, NodePosition nodePosition) {
        super(nodePosition);
        this.withRowAccessPolicy = withRowAccessPolicy;
    }

    public WithRowAccessPolicy getRowAccessPolicyContext() {
        return withRowAccessPolicy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitApplyRowAccessPolicyClause(this, context);
        } else {
            return null;
        }
    }
}
