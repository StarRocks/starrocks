// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class RevokeRowAccessPolicyClause extends AlterTableClause {
    private final PolicyName policyName;

    public RevokeRowAccessPolicyClause(PolicyName policyName, NodePosition nodePosition) {
        super(AlterOpType.REVOKE_ROW_ACCESS_POLICY, nodePosition);
        this.policyName = policyName;
    }

    public RevokeRowAccessPolicyClause(NodePosition nodePosition) {
        super(AlterOpType.REVOKE_ALL_ROW_ACCESS_POLICY, nodePosition);
        this.policyName = null;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRevokeRowAccessPolicyClause(this, context);
    }
}
