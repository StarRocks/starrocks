// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.mysql.privilege.Role;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.StatementBase;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class PrivilegeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        if (session.getGlobalStateMgr().isUsingNewPrivilege()) {
            PrivilegeStmtAnalyzerV2.analyze(statement, session);
        } else {
            new PrivilegeStatementAnalyzerVisitor().analyze(statement, session);
        }
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
                if (!session.getGlobalStateMgr().getAuth().doesUserExist(userIdent)) {
                    throw new SemanticException("user " + userIdent + " not exist!");
                }
            }
        }

        /**
         * check if role name valid and get full role name + check if role exists
         */
        private void analyseRoleName(String roleName, ConnectContext session, boolean canBeAdmin, String errMsg) {
            validRoleName(roleName, canBeAdmin, errMsg);
            if (!session.getGlobalStateMgr().getAuth().doesRoleExist(roleName)) {
                throw new SemanticException("role " + roleName + " not exist!");
            }
        }

        /**
         * check if role name valid
         */
        private void validRoleName(String roleName, boolean canBeAdmin, String errMsg) {
            try {
                FeNameFormat.checkRoleName(roleName, canBeAdmin, errMsg);
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
            if (stmt.getUserIdent() == null) {
                throw new SemanticException("Unsupported syntax: grant/revoke to role is not supported");
            }
            analyseUser(stmt.getUserIdent(), session, true);
            analyseRoleName(stmt.getGranteeRole(), session, true, "Can not granted/revoke role to user");
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            if (stmt.isWithGrantOption()) {
                throw new SemanticException("unsupported syntax: WITH GRANT OPTION");
            }
            // validate user/role
            if (stmt.getUserIdentity() != null) {
                analyseUser(stmt.getUserIdentity(), session, true);
            } else {
                analyseRoleName(stmt.getRole(), session, true, "invalid role");
            }

            // parse privilege actions to PrivBitSet
            PrivBitSet privs = getPrivBitSet(stmt.getPrivList(), stmt);
            String privType = stmt.getPrivType();
            if (privType.equals("TABLE") || privType.equals("DATABASE")) {
                if (stmt.getPrivilegeObjectNameTokensList().size() != 1) {
                    throw new SemanticException("unsupported syntax: can only grant/revoke on one " + privType);
                }
                analyseTablePrivs(stmt, privs, stmt.getPrivilegeObjectNameTokensList().get(0));
            } else if (privType.equals("RESOURCE")) {
                if (stmt.getPrivilegeObjectNameTokensList().size() != 1) {
                    throw new SemanticException("unsupported syntax: can only grant/revoke on one " + privType);
                }
                analyseResourcePrivs(stmt, privs, stmt.getPrivilegeObjectNameTokensList().get(0));
            } else if (privType.equals("USER")) {
                if (stmt.getPrivList().size() != 1 || !privs.containsPrivs(Privilege.IMPERSONATE_PRIV)) {
                    throw new SemanticException("only IMPERSONATE can only be granted on user");
                }
                if (stmt.getUserPrivilegeObjectList().size() != 1) {
                    throw new SemanticException("unsupported syntax: can only grant/revoke on one USER");
                }
                stmt.setPrivBitSet(privs);
                analyseUser(stmt.getUserPrivilegeObjectList().get(0), session, true);
            } else {
                throw new SemanticException("unsupported privilege type " + privType);
            }

            return null;
        }

        @NotNull
        private static PrivBitSet getPrivBitSet(List<String> privList, BaseGrantRevokePrivilegeStmt stmt) {
            if (privList.isEmpty()) {
                throw new SemanticException("No privileges in grant statement.");
            }
            PrivBitSet privs = PrivBitSet.of();
            for (String privStr : privList) {
                AccessPrivilege accessPrivilege;
                // for xc only
                if (stmt.getPrivType().equals("DATABASE") &&
                        stmt.getPrivilegeObjectNameTokensList().get(0).get(0).equalsIgnoreCase(
                                PropertyAnalyzer.PROPERTIES_XC_MAC_ACCESS_LABEL)) {
                    if (privStr.equalsIgnoreCase("normal")) {
                        privStr = "CREATE_PRIV";
                    } else if (privStr.equalsIgnoreCase("secret")) {
                        privStr = "ALTER_PRIV";
                    } else if (privStr.equalsIgnoreCase("top_secret")) {
                        privStr = "DROP_PRIV";
                    }
                }
                try {
                    accessPrivilege = AccessPrivilege.valueOf(privStr);
                } catch (IllegalArgumentException e) {
                    try {
                        // SELECT -> SELECT_PRIV
                        accessPrivilege = AccessPrivilege.valueOf(privStr + "_PRIV");
                    } catch (IllegalArgumentException e1) {
                        throw new SemanticException("Unknown privilege " + privStr);
                    }
                }
                if (accessPrivilege == null) {
                    throw new SemanticException("Unknown privilege " + privStr);
                }
                privs.or(accessPrivilege.toPrivilege());
            }
            return privs;
        }

        private static void analyseResourcePrivs(BaseGrantRevokePrivilegeStmt stmt, PrivBitSet privs,
                                                 List<String> privilegeObjectList) {
            if (privilegeObjectList == null || privilegeObjectList.size() != 1) {
                throw new SemanticException("invalid resource pattern!");
            }
            ResourcePattern resourcePattern = new ResourcePattern(privilegeObjectList.get(0));
            try {
                resourcePattern.analyze();
            } catch (AnalysisException e) {
                SemanticException exception = new SemanticException("invalid resource pattern " + resourcePattern);
                exception.initCause(e);
                throw exception;
            }
            if (resourcePattern.getPrivLevel() != Auth.PrivLevel.GLOBAL
                    && privs.containsPrivs(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV)) {
                throw new SemanticException(privs.toPrivilegeList() + " can only be granted on resource *");
            }
            stmt.setAnalysedResource(privs, resourcePattern);
        }

        private static void analyseTablePrivs(BaseGrantRevokePrivilegeStmt stmt, PrivBitSet privs,
                                              List<String> privilegeObjectList) {
            if (privilegeObjectList == null) {
                throw new SemanticException("invalid table pattern!");
            }
            TablePattern tablePattern;
            if (privilegeObjectList.size() == 1) {
                tablePattern = new TablePattern(privilegeObjectList.get(0), "*");
            } else {
                String db = privilegeObjectList.get(0);
                if (db.equalsIgnoreCase(PropertyAnalyzer.PROPERTIES_XC_MAC_ACCESS_LABEL)) {
                    db = PropertyAnalyzer.PROPERTIES_XC_MAC_ACCESS_LABEL;
                }
                tablePattern = new TablePattern(db, privilegeObjectList.get(1));
            }
            try {
                tablePattern.analyze();
            } catch (AnalysisException e) {
                SemanticException exception = new SemanticException("invalid table pattern " + tablePattern.toString());
                exception.initCause(e);
                throw exception;
            }
            if (tablePattern.getPrivLevel() != Auth.PrivLevel.GLOBAL
                    && privs.containsPrivs(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV)) {
                throw new SemanticException(privs.toPrivilegeList() + " can only be granted on table *.*");
            }
            stmt.setAnalysedTable(privs, tablePattern);
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

        public Void visitCreateAlterUserStatement(BaseCreateAlterUserStmt stmt, ConnectContext session) {
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
                analyseRoleName(stmt.getQualifiedRole(), session, true, "Can not granted/revoke role to user");
            }
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdentity(), session, false);
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext session) {
            validRoleName(stmt.getQualifiedRole(), false, "Can not create role");
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt stmt, ConnectContext session) {
            if (stmt.getUserIdent() != null) {
                if (stmt.isAll()) {
                    throw new SemanticException("Can not specified keyword ALL when specified user");
                }
                analyseUser(stmt.getUserIdent(), session, true);
            } else {
                if (!stmt.isAll()) {
                    // self
                    stmt.setUserIdent(session.getCurrentUserIdentity());
                }
            }
            Preconditions.checkState(stmt.isAll() || session.getCurrentUserIdentity() != null);
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            validRoleName(stmt.getQualifiedRole(), false, "Can not drop role");
            return null;
        }
    }
}
