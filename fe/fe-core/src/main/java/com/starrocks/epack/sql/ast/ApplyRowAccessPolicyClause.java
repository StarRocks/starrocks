// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class ApplyRowAccessPolicyClause extends AlterTableClause {
    private final WithRowAccessPolicy withRowAccessPolicy;

    public ApplyRowAccessPolicyClause(WithRowAccessPolicy withRowAccessPolicy, NodePosition nodePosition) {
        super(AlterOpType.APPLY_ROW_ACCESS_POLICY, nodePosition);
        this.withRowAccessPolicy = withRowAccessPolicy;
    }

    public WithRowAccessPolicy getRowAccessPolicyContext() {
        return withRowAccessPolicy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitApplyRowAccessPolicyClause(this, context);
    }
}
