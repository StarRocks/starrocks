// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.RevokeStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
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
        private void analyseUserAndCheckExist(UserIdentity userIdent, ConnectContext session) {
            // validate user
            analyseUser(userIdent);

            // check if user exists
            if (!session.getGlobalStateMgr().getAuth().getUserPrivTable().doesUserExist(userIdent)) {
                throw new SemanticException("user " + userIdent + " not exist!");
            }
        }

        /**
         *  analyse user identity
         */
        private void analyseUser(UserIdentity userIdent) {
            // validate user
            try {
                userIdent.analyze();
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
        }

        /**
         * check if role name valid and get full role name
         */
        private String analyseRoleName(String roleName, ConnectContext session, boolean canBeAdmin, String errMsg) {
            String qualifiedRole = validRoleName(roleName, canBeAdmin, errMsg);
            if (!session.getGlobalStateMgr().getAuth().doesRoleExist(qualifiedRole)) {
                throw new SemanticException("role " + qualifiedRole + " not exist!");
            }
            return qualifiedRole;
        }

        /**
         * check if role name valid and get full role name
         */
        private String validRoleName(String roleName, boolean canBeAdmin, String errMsg) {
            try {
                FeNameFormat.checkRoleName(roleName, canBeAdmin, errMsg);
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
            return ClusterNamespace.getFullName(roleName);
        }

        private void analyseTablePattern(TablePattern tablePattern, ConnectContext session) {
            try {
                tablePattern.analyze();
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
        }

        private void analyseResourcePattern(ResourcePattern resourcePattern) {
            try {
                resourcePattern.analyze();
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
        }

        /**
         * GRANT rolexx to userxx
         * REVOKE rolexx from userxx
         */
        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            analyseUserAndCheckExist(stmt.getUserIdent(), session);
            stmt.setQualifiedRole(
                    analyseRoleName(stmt.getRole(), session, true, "Can not granted/revoke role to user"));
            return null;
        }

        /**
         * GRANT IMPERSONATE ON XX TO XX
         * REVOKE IMPERSONATE ON XX TO XX
         */
        @Override
        public Void visitGrantRevokeImpersonateStatement(BaseGrantRevokeImpersonateStmt stmt, ConnectContext session) {
            analyseUserAndCheckExist(stmt.getAuthorizedUser(), session);
            analyseUserAndCheckExist(stmt.getSecuredUser(), session);
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
            analyseUserAndCheckExist(stmt.getToUser(), session);
            return null;
        }

        @Override
        public Void visitAlterUserStatement(AlterUserStmt stmt, ConnectContext session) {
            analyseUserAndCheckExist(stmt.getUserIdent(), session);
            /*
             * IDENTIFIED BY
             */
            // convert password to hashed password
            byte[] scramblePassword;
            if (!Strings.isNullOrEmpty(stmt.getOriginalPassword())) {
                try {
                    if (stmt.isPasswordPlain()) {
                        // plain password should check for validation & reuse
                        Auth.validatePassword(stmt.getOriginalPassword());
                        GlobalStateMgr.getCurrentState().getAuth()
                                .checkPasswordReuse(stmt.getUserIdent(), stmt.getOriginalPassword());
                        // convert plain password to scramble
                        scramblePassword = MysqlPassword.makeScrambledPassword(stmt.getOriginalPassword());
                    } else {
                        scramblePassword = MysqlPassword.checkPassword(stmt.getOriginalPassword());
                    }
                } catch (DdlException | AnalysisException e) {
                    // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                    // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                    // Remove it after all old methods migrate to the new framework
                    throw new SemanticException(e.getMessage());
                }
            } else {
                scramblePassword = new byte[0];
            }
            stmt.setScramblePassword(scramblePassword);

            /*
             * IDENTIFIED WITH
             */
            if (!Strings.isNullOrEmpty(stmt.getAuthPlugin())) {
                if (AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name().equals(stmt.getAuthPlugin())) {
                    stmt.setUserForAuthPlugin(stmt.getAuthString());
                } else if (AuthPlugin.MYSQL_NATIVE_PASSWORD.name().equals(stmt.getAuthPlugin())) {
                    // in this case, authString is password
                    // convert password to hashed password
                    if (!Strings.isNullOrEmpty(stmt.getAuthString())) {
                        if (stmt.isPasswordPlain()) {
                            // convert plain password to scramble
                            scramblePassword = MysqlPassword.makeScrambledPassword(stmt.getAuthString());
                        } else {
                            try {
                                scramblePassword = MysqlPassword.checkPassword(stmt.getAuthString());
                            } catch (AnalysisException e) {
                                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                                // Remove it after all old methods migrate to the new framework
                                throw new SemanticException(e.getMessage());
                            }
                        }
                    } else {
                        scramblePassword = new byte[0];
                    }
                    stmt.setScramblePassword(scramblePassword);
                } else if (AuthPlugin.AUTHENTICATION_KERBEROS.name().equals(stmt.getAuthPlugin()) &&
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
            return null;
        }

        @Override
        public Void visitGrantPrivilegeStatement(GrantStmt stmt, ConnectContext session) {
            if (stmt.getUserIdent() != null) {
                analyseUser(stmt.getUserIdent());
            } else {
                stmt.setQualifiedRole(validRoleName(stmt.getQualifiedRole(), false, "Can not grant to role"));
            }

            if (stmt.getTblPattern() != null) {
                analyseTablePattern(stmt.getTblPattern(), session);
            } else {
                analyseResourcePattern(stmt.getResourcePattern());
            }

            if (!stmt.hasPrivileges()) {
                throw new SemanticException("No privileges in grant statement.");
            }

            return null;
        }

        @Override
        public Void visitRevokePrivilegeStatement(RevokeStmt stmt, ConnectContext session) {
            if (stmt.getUserIdent() != null) {
                analyseUser(stmt.getUserIdent(), session);
            } else {
                stmt.setQualifiedRole(validRoleName(stmt.getQualifiedRole(), false, "Can not revoke from role"));
            }

            if (stmt.getTblPattern() != null) {
                analyseTablePattern(stmt.getTblPattern(), session);
            } else {
                analyseResourcePattern(stmt.getResourcePattern());
            }

            if (!stmt.hasPrivileges()) {
                throw new SemanticException("No privileges in revoke statement.");
            }
            return null;
        }
    }
}
