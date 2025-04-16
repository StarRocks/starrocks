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

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Arrays;

public class AuthenticationAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AuthenticationAnalyzerVisitor().analyze(statement, session);
    }

    public static class AuthenticationAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        private AuthenticationMgr authenticationManager = null;
        private AuthorizationMgr authorizationManager = null;

        public void analyze(StatementBase statement, ConnectContext session) {
            authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            visit(statement, session);
        }

        /**
         * analyse user identity + check if user exists in UserPrivTable
         */
        private void analyseUser(UserIdentity userIdent, boolean checkExist) {
            userIdent.analyze();

            // check if user exists
            if (checkExist && !authenticationManager.doesUserExist(userIdent)) {
                throw new SemanticException("cannot find user " + userIdent + "!");
            }
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

        @Override
        public Void visitCreateUserStatement(CreateUserStmt stmt, ConnectContext context) {
            stmt.getUserIdentity().analyze();
            if (authenticationManager.doesUserExist(stmt.getUserIdentity()) && !stmt.isIfNotExists()) {
                throw new SemanticException("Operation CREATE USER failed for " + stmt.getUserIdentity()
                        + " : user already exists");
            }
            if (!stmt.getDefaultRoles().isEmpty()) {
                stmt.getDefaultRoles().forEach(r -> validRoleName(r, "Valid role name fail", true));
            }

            UserAuthenticationInfo userAuthenticationInfo = analyzeAuthOption(stmt.getUserIdentity(), stmt.getAuthOption());
            stmt.setAuthenticationInfo(userAuthenticationInfo);
            return null;
        }

        @Override
        public Void visitAlterUserStatement(AlterUserStmt stmt, ConnectContext context) {
            stmt.getUserIdentity().analyze();
            if (!authenticationManager.doesUserExist(stmt.getUserIdentity()) && !stmt.isIfExists()) {
                throw new SemanticException("Operation ALTER USER failed for " + stmt.getUserIdentity()
                        + " : user not exists");
            }

            UserAuthenticationInfo userAuthenticationInfo = analyzeAuthOption(stmt.getUserIdentity(), stmt.getAuthOption());
            stmt.setAuthenticationInfo(userAuthenticationInfo);
            return null;
        }

        private UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption) {
            String authPluginUsing;
            if (userAuthOption == null || userAuthOption.getAuthPlugin() == null) {
                authPluginUsing = AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString();
            } else {
                authPluginUsing = userAuthOption.getAuthPlugin();
            }
            String authString = userAuthOption == null ? null : userAuthOption.getAuthString();
            AuthenticationProvider provider = AuthenticationProviderFactory.create(authPluginUsing, authString);
            if (provider == null) {
                throw new SemanticException("Cannot find " + authPluginUsing
                        + " from " + Arrays.toString(AuthPlugin.Client.values()));
            }
            try {
                return provider.analyzeAuthOption(userIdentity, userAuthOption);
            } catch (AuthenticationException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        private boolean needProtectAdminUser(UserIdentity userIdentity, ConnectContext context) {
            return Config.authorization_enable_admin_user_protection &&
                    userIdentity.getUser().equalsIgnoreCase("admin") &&
                    !context.getCurrentUserIdentity().equals(UserIdentity.ROOT);
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            UserIdentity userIdentity = stmt.getUserIdentity();
            userIdentity.analyze();

            if (needProtectAdminUser(userIdentity, session)) {
                throw new SemanticException("'admin' user cannot be dropped because of " +
                        "'authorization_enable_admin_user_protection' configuration is enabled");
            }

            if (!authenticationManager.doesUserExist(userIdentity) && !stmt.isIfExists()) {
                throw new SemanticException("Operation DROP USER failed for " + userIdentity + " : user not exists");
            }

            if (stmt.getUserIdentity().equals(UserIdentity.ROOT)) {
                throw new SemanticException("Operation DROP USER failed for " + UserIdentity.ROOT +
                        " : cannot drop user " + UserIdentity.ROOT);
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if (user != null) {
                analyseUser(user, true);
            } else if (!statement.isAll()) {
                statement.setUserIdent(context.getCurrentUserIdentity());
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            if (stmt.isAllowRevert()) {
                throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
            }
            analyseUser(stmt.getToUser(), true);
            return null;
        }
    }
}
