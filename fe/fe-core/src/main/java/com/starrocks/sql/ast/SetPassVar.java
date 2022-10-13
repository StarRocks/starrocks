// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

public class SetPassVar extends SetVar {
    private UserIdentity userIdent;
    private final String passwdParam;
    private byte[] passwdBytes;

    // The password in parameter is a hashed password.
    public SetPassVar(UserIdentity userIdent, String passwd) {
        this.userIdent = userIdent;
        this.passwdParam = passwd;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public byte[] getPassword() {
        return passwdBytes;
    }

    @Override
    public void analyze() {
        boolean isSelf = false;
        ConnectContext ctx = ConnectContext.get();
        if (userIdent == null) {
            // set userIdent as what current_user() returns
            userIdent = ctx.getCurrentUserIdentity();
            isSelf = true;
        } else {
            try {
                userIdent.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            if (userIdent.equals(ctx.getCurrentUserIdentity())) {
                isSelf = true;
            }
        }

        try {
            passwdBytes = MysqlPassword.checkPassword(passwdParam);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }

        // check privs.
        // 1. this is user itself
        if (isSelf) {
            return;
        }

        // 2. No user can set password for root expect for root user itself
        if (userIdent.getQualifiedUser().equals(Auth.ROOT_USER)
                && !ctx.getQualifiedUser().equals(Auth.ROOT_USER)) {
            throw new SemanticException("Can not set password for root user, except root itself");
        }

        // 3. user has grant privs
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }
}
