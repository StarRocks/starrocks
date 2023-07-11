// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseVisitor;
import com.starrocks.sql.analyzer.SemanticException;

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
        PolicyName policyName = clause.getPolicyName();
        AnalyzerUtilsEPack.normalizationPolicyName(context, policyName);

        SecurityPolicyMgr securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyManager.getPolicyByName(PolicyType.ROW_ACCESS, policyName);
        if (policy == null) {
            throw new SemanticException("Can't find masking policy : " + policyName.getName());
        }
        clause.setPolicyId(policy.getPolicyId());
        return null;
    }
}
