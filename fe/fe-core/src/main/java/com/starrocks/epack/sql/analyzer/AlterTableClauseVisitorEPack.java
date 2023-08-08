// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.alter.AlterOpType;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AlterTableClauseVisitor;

public class AlterTableClauseVisitorEPack extends AlterTableClauseVisitor {
    @Override
    public Void visitApplyMaskingPolicyClause(ApplyMaskingPolicyClause clause, ConnectContext context) {
        clause.getWithColumnMaskingPolicy().analyze(context);
        return null;
    }

    @Override
    public Void visitRevokeMaskingPolicyClause(RevokeMaskingPolicyClause clause, ConnectContext context) {
        return null;
    }

    @Override
    public Void visitApplyRowAccessPolicyClause(ApplyRowAccessPolicyClause clause, ConnectContext context) {
        clause.getRowAccessPolicyContext().analyze(context);
        return null;
    }

    @Override
    public Void visitRevokeRowAccessPolicyClause(RevokeRowAccessPolicyClause clause, ConnectContext context) {
        if (clause.getOpType().equals(AlterOpType.REVOKE_ROW_ACCESS_POLICY)) {
            PolicyName policyName = clause.getPolicyName();
            AnalyzerUtilsEPack.normalizationPolicyName(context, policyName);
        }
        return null;
    }
}
