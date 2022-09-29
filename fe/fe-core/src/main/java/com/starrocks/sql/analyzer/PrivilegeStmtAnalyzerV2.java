// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;

public class PrivilegeStmtAnalyzerV2 {
    private PrivilegeStmtAnalyzerV2() {
    }

    public static void analyze(StatementBase statement, ConnectContext session) {
        new PrivilegeStatementAnalyzerVisitor().analyze(statement, session);
    }

    static class PrivilegeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        private AuthenticationManager authenticationManager = null;

        public void analyze(StatementBase statement, ConnectContext session) {
            authenticationManager = session.getGlobalStateMgr().getAuthenticationManager();
            visit(statement, session);
        }

        /**
         * analyse user identity + check if user exists in UserPrivTable
         */
        private void analyseUser(UserIdentity userIdent, boolean checkExist) {
            // analyse user identity
            try {
                userIdent.analyze();
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }

            // check if user exists
            if (checkExist && !authenticationManager.doesUserExist(userIdent)) {
                throw new SemanticException("user " + userIdent + " not exist!");
            }
        }

        @Override
        public Void visitCreateAlterUserStatement(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdent(), stmt instanceof AlterUserStmt);

            if (stmt.getAuthPlugin() == null) {
                stmt.setAuthPlugin(authenticationManager.getDefaultPlugin());
            }
            try {
                AuthenticationProviderFactory.create(stmt.getAuthPlugin()).validAuthenticationInfo(
                        stmt.getUserIdent(), stmt.getOriginalPassword(), stmt.getAuthString());
            } catch (AuthenticationException e) {
                SemanticException exception = new SemanticException("invalidate authentication: " + e.getMessage());
                exception.initCause(e);
                throw exception;
            }

            if (stmt.hasRole()) {
                throw new SemanticException("role not supported!");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            // validate user/role
            if (stmt.getUserIdentity() != null) {
                analyseUser(stmt.getUserIdentity(), true);
            } else {
                // TODO
                throw new SemanticException("not supported");
            }
            return null;
        }

    }
}
