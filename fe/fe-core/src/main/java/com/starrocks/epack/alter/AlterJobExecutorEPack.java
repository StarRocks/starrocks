// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.alter;

import com.starrocks.alter.AlterJobExecutor;
import com.starrocks.analysis.TableName;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

public class AlterJobExecutorEPack extends AlterJobExecutor implements AstVisitorEPack<Void, ConnectContext> {
    //Apply Policy clause
    @Override
    public Void visitApplyMaskingPolicyClause(ApplyMaskingPolicyClause alterClause, ConnectContext context) {
        GlobalStateMgr.getCurrentState().getSecurityPolicyManager().applyMaskingPolicyContext(
                context,
                new TableName(this.catalog, db.getFullName(), table.getName()),
                alterClause.getMaskingColumn(),
                alterClause.getWithColumnMaskingPolicy());
        return null;
    }

    @Override
    public Void visitRevokeMaskingPolicyClause(RevokeMaskingPolicyClause alterClause, ConnectContext context) {
        GlobalStateMgr.getCurrentState().getSecurityPolicyManager().revokeMaskingPolicyContext(
                context,
                this.catalog, db.getFullName(), table.getName(),
                alterClause.getMaskingColumn());
        return null;
    }

    @Override
    public Void visitApplyRowAccessPolicyClause(ApplyRowAccessPolicyClause alterClause, ConnectContext context) {
        GlobalStateMgr.getCurrentState().getSecurityPolicyManager().applyRowAccessPolicyContext(
                context,
                new TableName(this.catalog, db.getFullName(), table.getName()),
                alterClause.getRowAccessPolicyContext());
        return null;
    }

    @Override
    public Void visitRevokeRowAccessPolicyClause(RevokeRowAccessPolicyClause alterClause, ConnectContext context) {
        if (alterClause.isRevokeAll()) {
            GlobalStateMgr.getCurrentState().getSecurityPolicyManager().revokeALLRowAccessPolicyContext(
                    context, this.catalog, db.getFullName(), table.getName());
        } else {
            GlobalStateMgr.getCurrentState().getSecurityPolicyManager().revokeRowAccessPolicyContext(
                    context, this.catalog, db.getFullName(), table.getName(), alterClause.getPolicyName());
        }
        return null;
    }
}
