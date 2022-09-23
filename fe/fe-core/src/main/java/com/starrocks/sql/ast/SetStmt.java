// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

import java.util.List;

public class SetStmt extends StatementBase {
    private final List<SetVar> setVars;

    public SetStmt(List<SetVar> setVars) {
        this.setVars = setVars;
    }

    public List<SetVar> getSetVars() {
        return setVars;
    }

    @Override
    public boolean needAuditEncryption() {
        for (SetVar var : setVars) {
            if (var instanceof SetPassVar) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (setVars != null) {
            for (SetVar var : setVars) {
                if (var instanceof SetPassVar) {
                    return RedirectStatus.FORWARD_WITH_SYNC;
                } else if (var.getType() == SetType.GLOBAL) {
                    return RedirectStatus.FORWARD_WITH_SYNC;
                }
            }
        }
        return RedirectStatus.NO_FORWARD;
    }

    private boolean isSupportNewPlanner() {
        return setVars.stream().noneMatch(var -> var instanceof SetTransaction);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetStatement(this, context);
    }
}

