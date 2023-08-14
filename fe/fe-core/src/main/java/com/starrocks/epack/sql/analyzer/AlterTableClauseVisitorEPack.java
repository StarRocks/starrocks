// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.alter.AlterOpType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseVisitor;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;

public class AlterTableClauseVisitorEPack extends AlterTableClauseVisitor {
    @Override
    public Void visitApplyMaskingPolicyClause(ApplyMaskingPolicyClause clause, ConnectContext context) {
        clause.getWithColumnMaskingPolicy().analyze(context, clause.getMaskingColumn());
        WithColumnMaskingPolicy withColumnMaskingPolicy = clause.getWithColumnMaskingPolicy();
        analyzeApplyPolicy(withColumnMaskingPolicy.getPolicyName(), withColumnMaskingPolicy.getUsingColumns(),
                clause.getMaskingColumn(), PolicyType.MASKING);

        return null;
    }

    @Override
    public Void visitRevokeMaskingPolicyClause(RevokeMaskingPolicyClause clause, ConnectContext context) {
        return null;
    }

    @Override
    public Void visitApplyRowAccessPolicyClause(ApplyRowAccessPolicyClause clause, ConnectContext context) {
        clause.getRowAccessPolicyContext().analyze(context);
        WithRowAccessPolicy withRowAccessPolicy = clause.getRowAccessPolicyContext();
        analyzeApplyPolicy(withRowAccessPolicy.getPolicyName(), withRowAccessPolicy.getOnColumns(), null, PolicyType.ROW_ACCESS);

        return null;
    }

    private void analyzeApplyPolicy(PolicyName policyName, List<String> usingColumns,
                                    String maskingColumnName, PolicyType policyType) {
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyMgr.getPolicyByName(policyType, policyName);

        Table table = this.getTable();
        for (int i = 0; i < usingColumns.size(); ++i) {
            Column column = table.getColumn(usingColumns.get(i));
            if (column == null) {
                throw new SemanticException("Column " + usingColumns.get(i) + " is not exist in table " + table.getName());
            }

            if (!Type.canCastTo(column.getType(), policy.getArgTypes().get(i))) {
                throw new SemanticException("Can't cast param type from " + column.getType()
                        + " to " + policy.getArgTypes().get(i));
            }
        }

        if (policyType.equals(PolicyType.MASKING)) {
            Column maskingColumn = table.getColumn(maskingColumnName);
            Type targetType = maskingColumn.getType();
            if (!Type.canCastTo(policy.getRetType(), targetType)) {
                throw new SemanticException("Can't cast return type from " + policy.getRetType() + " to " + targetType);
            }
        }

        if (!table.getRelatedMaterializedViews().isEmpty()) {
            throw new SemanticException("Can not apply policy to table which has related materialized view");
        }
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
