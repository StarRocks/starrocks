// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class RevokeRowAccessPolicyClause extends AlterTableClause {
    private final boolean isRevokeAll;
    private final PolicyName policyName;

    public RevokeRowAccessPolicyClause(PolicyName policyName, NodePosition nodePosition) {
        super(nodePosition);
        this.isRevokeAll = false;
        this.policyName = policyName;
    }

    public RevokeRowAccessPolicyClause(NodePosition nodePosition) {
        super(nodePosition);
        this.isRevokeAll = true;
        this.policyName = null;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public boolean isRevokeAll() {
        return isRevokeAll;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitRevokeRowAccessPolicyClause(this, context);
        } else {
            return null;
        }
    }
}
