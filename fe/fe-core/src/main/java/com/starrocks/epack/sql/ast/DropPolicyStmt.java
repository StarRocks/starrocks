// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class DropPolicyStmt extends DdlStmt {
    private final PolicyType policyType;
    private final PolicyName policyName;
    private final boolean ifExists;
    private final boolean force;

    public DropPolicyStmt(PolicyType policyType, PolicyName policyName, boolean ifExists, boolean force,
                          NodePosition nodePosition) {
        super(nodePosition);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifExists = ifExists;
        this.force = force;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public boolean isForce() {
        return force;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropPolicyStatement(this, context);
    }
}
