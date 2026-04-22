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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserProperty;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.Config;
import com.starrocks.common.PatternMatcher;
import com.starrocks.epack.authorization.PasswordPolicy;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;

import java.util.Map;
import java.util.Map.Entry;

public class AuthenticationAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AuthenticationAnalyzerVisitor().analyze(statement, session);
    }

    public static void analyzeUser(UserRef userIdent) {
        analyzeUser(userIdent, false);
    }

    public static void analyzeUser(UserRef userIdent, boolean allowEmail) {
        String user = userIdent.getUser();
        String host = userIdent.getHost();

        if (Strings.isNullOrEmpty(user)) {
            throw new SemanticException("Does not support anonymous user");
        }

        FeNameFormat.checkUserName(user, allowEmail);

        // reuse createMysqlPattern to validate host pattern
        PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());
    }

    // Auth plugins where the StarRocks username must literally equal an external IdP
    // principal claim (JWT 'sub' / 'email' / 'preferred_username', OAuth2 principal_field).
    // For these, relax the local username regex to accept email-format identifiers.
    // Password-based plugins (mysql_native_password), LDAP, and Kerberos still use the
    // strict regex.
    private static boolean allowsEmailUsername(UserAuthOption authOption) {
        if (authOption == null) {
            return false;
        }
        String plugin = authOption.getAuthPlugin();
        return AuthPlugin.Server.AUTHENTICATION_OAUTH2.toString().equalsIgnoreCase(plugin)
                || AuthPlugin.Server.AUTHENTICATION_JWT.toString().equalsIgnoreCase(plugin);
    }

    public static void checkUserExist(UserRef user, boolean checkExist) {
        AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        UserIdentity userIdent = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
        if (checkExist && !authenticationManager.doesUserExist(userIdent)) {
            throw new SemanticException("cannot find user " + userIdent + "!");
        }
    }

    public static void checkUserNotExist(UserRef user, boolean checkNotExist) {
        AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        UserIdentity userIdent = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
        if (!checkNotExist && authenticationManager.doesUserExist(userIdent)) {
            throw new SemanticException("user " + userIdent + " already exists!");
        }
    }

    public static boolean needProtectAdminUser(UserRef user, ConnectContext context) {
        return Config.authorization_enable_admin_user_protection &&
                user.getUser().equalsIgnoreCase("admin") &&
                !context.getCurrentUserIdentity().equals(UserIdentity.ROOT);
    }

    public static class AuthenticationAnalyzerVisitor implements AstVisitorExtendInterface<Void, ConnectContext> {
        private AuthorizationMgr authorizationManager = null;

        public void analyze(StatementBase statement, ConnectContext session) {
            authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            visit(statement, session);
        }

        /**
         * check if role name valid and get full role name
         */
        private void validRoleName(String roleName, String errMsg, boolean checkExist) {
            // always set to true, we can validate if it's allowed to operation on admin later
            FeNameFormat.checkRoleName(roleName, true, errMsg);
            // check if role exists
            if (checkExist && !authorizationManager.checkRoleExists(roleName)) {
                throw new SemanticException(errMsg + ": cannot find role " + roleName + "!");
            }
        }

        private PasswordPolicy getPasswordPolicyFromProperties(Map<String, String> properties) {
            if (properties == null || properties.isEmpty()) {
                return null;
            }

            for (Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(UserProperty.PROP_PASSWORD_POLICY)) {
                    String policyName = entry.getValue();
                    if (policyName.equalsIgnoreCase(UserProperty.EMPTY_VALUE)) {
                        return null;
                    }

                    PasswordPolicy passwordPolicy = GlobalStateMgr.getCurrentState()
                            .getSecurityPolicyManager().getPasswordPolicy(policyName);
                    if (passwordPolicy == null) {
                        throw new SemanticException("Password Policy " + policyName + " not exist");
                    }
                    return passwordPolicy;
                }
            }

            return null;
        }

        @Override
        public Void visitCreateUserStatement(CreateUserStmt stmt, ConnectContext context) {
            analyzeUser(stmt.getUser(), allowsEmailUsername(stmt.getAuthOption()));
            checkUserNotExist(stmt.getUser(), stmt.isIfNotExists());
            if (!stmt.getDefaultRoles().isEmpty()) {
                stmt.getDefaultRoles().forEach(r -> validRoleName(r, "Valid role name fail", true));
            }

            PasswordPolicy passwordPolicy = getPasswordPolicyFromProperties(stmt.getProperties());
            if (passwordPolicy == null) {
                passwordPolicy = GlobalStateMgr.getCurrentState().getSecurityPolicyManager().getGlobalPasswordPolicy();
            }

            UserAuthOptionAnalyzer.analyzeAuthOption(
                    stmt.getUser(), stmt.getAuthOption(), stmt.getPasswordOption(), passwordPolicy);
            return null;
        }

        @Override
        public Void visitAlterUserStatement(AlterUserStmt stmt, ConnectContext context) {
            analyzeUser(stmt.getUser(), true);
            checkUserExist(stmt.getUser(), !stmt.isIfExists());
            UserIdentity userIdentity = new UserIdentity(stmt.getUser().getUser(), stmt.getUser().getHost(),
                    stmt.getUser().isDomain());

            UserAuthOptionAnalyzer.analyzeAuthOption(stmt.getUser(), stmt.getAuthOption(), stmt.getPasswordOption(),
                    GlobalStateMgr.getCurrentState().getAuthenticationMgr().getEffectivePasswordPolicy(userIdentity));
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            UserRef user = stmt.getUser();
            analyzeUser(user, true);
            checkUserExist(user, !stmt.isIfExists());

            if (needProtectAdminUser(user, session)) {
                throw new SemanticException("'admin' user cannot be dropped because of " +
                        "'authorization_enable_admin_user_protection' configuration is enabled");
            }

            if (stmt.getUser().equals(UserRef.ROOT)) {
                throw new SemanticException("Operation DROP USER failed for " + UserIdentity.ROOT +
                        " : cannot drop user " + UserIdentity.ROOT);
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            UserRef user = statement.getUser();
            if (user != null) {
                analyzeUser(user, true);
                checkUserExist(user, true);
            } else if (!statement.isAll()) {
                statement.setUser(new UserRef(context.getCurrentUserIdentity().getUser(),
                        context.getCurrentUserIdentity().getHost(),
                        context.getCurrentUserIdentity().isDomain()));
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            if (stmt.isAllowRevert()) {
                throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
            }
            analyzeUser(stmt.getToUser(), true);
            // Skip existence check for external users since they don't exist in local storage
            if (!stmt.getToUser().isExternal()) {
                checkUserExist(stmt.getToUser(), true);
            }
            return null;
        }
    }
}
