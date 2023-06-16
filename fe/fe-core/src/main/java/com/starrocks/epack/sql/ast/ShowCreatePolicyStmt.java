// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowCreatePolicyStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    private final PolicyType policyType;
    private final PolicyName policyName;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Policy", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Create Policy", ScalarType.createVarchar(100)));
        META_DATA = builder.build();
    }

    public ShowCreatePolicyStmt(PolicyType policyType, PolicyName policyName, NodePosition pos) {
        super(pos);
        this.policyType = policyType;
        this.policyName = policyName;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreatePolicyStatement(this, context);
    }
}
