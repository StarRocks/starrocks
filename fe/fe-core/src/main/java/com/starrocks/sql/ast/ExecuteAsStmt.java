// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UserIdentity;

// EXECUTE AS XX WITH NO REVERT
public class ExecuteAsStmt extends StatementBase {
    protected UserIdentity toUser;
    protected boolean allowRevert;

    public ExecuteAsStmt(UserIdentity toUser, boolean allowRevert) {
        this.toUser = toUser;
        this.allowRevert = allowRevert;
    }

    public UserIdentity getToUser() {
        return toUser;
    }

    public boolean isAllowRevert() {
        return allowRevert;
    }

    @Override
    public String toString() {
        String s = String.format("EXECUTE AS %s", this.toUser.toString());
        if (allowRevert) {
            return s;
        } else {
            return s + " WITH NO REVERT";
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExecuteAsStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
