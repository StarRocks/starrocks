// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.Role;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokeImpersonateStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;


public class PrivilegeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new PrivilegeStatementAnalyzerVisitor().analyze(statement, session);
    }

    static class PrivilegeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        /**
         * analyse user identity + check if user exists in UserPrivTable
         */
        private void analyseUser(UserIdentity userIdent, ConnectContext session, boolean checkExist) {
            // analyse user identity
            try {
                userIdent.analyze();
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }

            if (checkExist) {
                // check if user exists
                if (!session.getGlobalStateMgr().getAuth().getUserPrivTable().doesUserExist(userIdent)) {
                    throw new SemanticException("user " + userIdent + " not exist!");
                }
            }
        }

        /**
         * check if role name valid and get full role name
         */
        private String analyseRoleName(String roleName, ConnectContext session) {
            try {
                FeNameFormat.checkRoleName(roleName, true /* can be admin */, "Can not granted/revoke role to user");
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
            String qualifiedRole = ClusterNamespace.getFullName(roleName);
            if (!session.getGlobalStateMgr().getAuth().doesRoleExist(qualifiedRole)) {
                throw new SemanticException("role " + qualifiedRole + " not exist!");
            }
            return qualifiedRole;
        }

        /**
         * GRANT rolexx to userxx
         * REVOKE rolexx from userxx
         */
        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdent(), session, true);
            stmt.setQualifiedRole(analyseRoleName(stmt.getRole(), session));
            return null;
        }

        /**
         * GRANT IMPERSONATE ON XX TO XX
         * GRANT IMPERSONATE ON XX TO ROLE XX
         * REVOKE IMPERSONATE ON XX FROM XX
         * REVOKE IMPERSONATE ON XX FROM ROLE XX
         */
        @Override
        public Void visitGrantRevokeImpersonateStatement(BaseGrantRevokeImpersonateStmt stmt, ConnectContext session) {
            analyseUser(stmt.getSecuredUser(), session, true);
            if (stmt.getAuthorizedUser() != null) {
                analyseUser(stmt.getAuthorizedUser(), session, true);
            } else {
                String qulifiedRole = analyseRoleName(stmt.getAuthorizedRoleName(), session);
                stmt.setAuthorizedRoleName(qulifiedRole);
            }
            return null;
        }

        /**
         * analyse EXECUTE AS statment
         */
        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            if (stmt.isAllowRevert()) {
                throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
            }
            analyseUser(stmt.getToUser(), session, true);
            return null;
        }

        /**
         * convert password to hashed password
         */
        private byte[] analysePassword(UserIdentity userIdent, String originalPassword, boolean isPasswordPlain,
                                       boolean checkReuse) {
            if (Strings.isNullOrEmpty(originalPassword)) {
                return new byte[0];
            }
            try {
                if (isPasswordPlain) {
                    // plain password should check for validation & reuse
                    Auth.validatePassword(originalPassword);
                    if (checkReuse) {
                        GlobalStateMgr.getCurrentState().getAuth().checkPasswordReuse(userIdent, originalPassword);
                    }
                    // convert plain password to scramble
                    return MysqlPassword.makeScrambledPassword(originalPassword);
                } else {
                    return MysqlPassword.checkPassword(originalPassword);
                }
            } catch (DdlException | AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
        }

        public Void visitCreateAlterUserStmt(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdent(), session, stmt instanceof AlterUserStmt);
            /*
             * IDENTIFIED BY
             */
            stmt.setScramblePassword(
                    analysePassword(stmt.getUserIdent(), stmt.getOriginalPassword(), stmt.isPasswordPlain(),
                            stmt instanceof AlterUserStmt));
            /*
             * IDENTIFIED WITH
             */
            if (!Strings.isNullOrEmpty(stmt.getAuthPlugin())) {
                if (AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name().equals(stmt.getAuthPlugin())) {
                    stmt.setUserForAuthPlugin(stmt.getAuthString());
                } else if (AuthPlugin.MYSQL_NATIVE_PASSWORD.name().equals(stmt.getAuthPlugin())) {
                    // in this case, authString is password
                    stmt.setScramblePassword(analysePassword(stmt.getUserIdent(), stmt.getAuthString(),
                            stmt.isPasswordPlain(), stmt instanceof AlterUserStmt));
                } else if (AuthPlugin.AUTHENTICATION_KERBEROS.name().equalsIgnoreCase(stmt.getAuthPlugin()) &&
                        GlobalStateMgr.getCurrentState().getAuth().isSupportKerberosAuth()) {
                    // In kerberos authentication, userForAuthPlugin represents the user principal realm.
                    // If user realm is not specified when creating user, the service principal realm will be used as
                    // the user principal realm by default.
                    if (stmt.getAuthString() != null) {
                        stmt.setUserForAuthPlugin(stmt.getAuthString());
                    } else {
                        stmt.setUserForAuthPlugin(Config.authentication_kerberos_service_principal.split("@")[1]);
                    }
                } else {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_AUTH_PLUGIN_NOT_LOADED, stmt.getAuthPlugin());
                }
            }

            if (stmt.hasRole()) {
                if (stmt.getQualifiedRole().equalsIgnoreCase("SUPERUSER")) {
                    // for forward compatibility
                    stmt.setRole(Role.ADMIN_ROLE);
                }
                stmt.setRole(analyseRoleName(stmt.getQualifiedRole(), session));
            }
            return null;
        }
    }
}
