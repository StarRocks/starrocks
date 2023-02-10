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

import com.google.common.base.Strings;
import com.starrocks.analysis.UserDesc;
import com.starrocks.authentication.UserAuthenticationInfo;

// CreateUserStmt and AlterUserStmt share the same parameter and check logic
public class BaseCreateAlterUserStmt extends DdlStmt {
    protected UserIdentity userIdent;
    protected String password;
    protected byte[] scramblePassword;
    protected boolean isPasswordPlain;
    protected String authPlugin;
    protected String authString;
    protected String userForAuthPlugin;
    protected String role;
    private final String operationName;   // CREATE or ALTER
    // used in new RBAC privilege framework
    private UserAuthenticationInfo authenticationInfo = null;

    public BaseCreateAlterUserStmt(UserDesc userDesc, String prepositionName) {
        this.userIdent = userDesc.getUserIdent();
        this.password = userDesc.getPassword();
        this.isPasswordPlain = userDesc.isPasswordPlain();
        this.authPlugin = userDesc.getAuthPlugin();
        this.authString = userDesc.getAuthString();
        this.operationName = prepositionName;
    }

    public BaseCreateAlterUserStmt(UserDesc userDesc, String role, String prepositionName) {
        this(userDesc, prepositionName);
        this.role = role;
    }


    public String getOriginalPassword() {
        return password;
    }

    public boolean isPasswordPlain() {
        return isPasswordPlain;
    }

    public String getAuthString() {
        return authString;
    }

    public void setScramblePassword(byte[] scramblePassword) {
        this.scramblePassword = scramblePassword;
    }

    public void setAuthPlugin(String authPlugin) {
        this.authPlugin = authPlugin;
    }

    public void setUserForAuthPlugin(String userForAuthPlugin) {
        this.userForAuthPlugin = userForAuthPlugin;
    }

    public byte[] getPassword() {
        return scramblePassword;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public String getUserForAuthPlugin() {
        return userForAuthPlugin;
    }

    public boolean hasRole() {
        return role != null;
    }

    public String getQualifiedRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public UserAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public void setAuthenticationInfo(UserAuthenticationInfo authenticationInfo) {
        this.authenticationInfo = authenticationInfo;
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(operationName).append(" USER ").append(userIdent);
        if (!Strings.isNullOrEmpty(password)) {
            if (isPasswordPlain) {
                sb.append(" IDENTIFIED BY '").append("*XXX").append("'");
            } else {
                sb.append(" IDENTIFIED BY PASSWORD '").append(password).append("'");
            }
        }

        if (!Strings.isNullOrEmpty(authPlugin)) {
            sb.append(" IDENTIFIED WITH ").append(authPlugin);
            if (!Strings.isNullOrEmpty(authString)) {
                if (isPasswordPlain) {
                    sb.append(" BY '");
                } else {
                    sb.append(" AS '");
                }
                sb.append(authString).append("'");
            }
        }

        if (!Strings.isNullOrEmpty(role)) {
            sb.append(" DEFAULT ROLE '").append(role).append("'");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAlterUserStatement(this, context);
    }
}
