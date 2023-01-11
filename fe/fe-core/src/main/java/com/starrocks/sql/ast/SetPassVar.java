// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.MysqlPassword;
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
        if (userIdent.getQualifiedUser().equals(AuthenticationManager.ROOT_USER)
                && !ctx.getQualifiedUser().equals(AuthenticationManager.ROOT_USER)) {
            throw new SemanticException("Can not set password for root user, except root itself");
        }

        // 3. user has grant privs
        if (!GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
    }
}
