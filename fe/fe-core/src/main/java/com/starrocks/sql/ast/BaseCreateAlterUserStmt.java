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

import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.UserAuthenticationInfo;

import java.util.List;

// CreateUserStmt and AlterUserStmt share the same parameter and check logic
public class BaseCreateAlterUserStmt extends DdlStmt {
    protected UserIdentity userIdent;
    protected String password;
    protected byte[] scramblePassword;
    protected boolean isPasswordPlain;
    protected String authPlugin;
    protected String authString;
    protected String userForAuthPlugin;

    protected SetRoleType setRoleType;
    protected List<String> defaultRoles;
    // used in new RBAC privilege framework
    private UserAuthenticationInfo authenticationInfo = null;

    public BaseCreateAlterUserStmt(UserDesc userDesc, SetRoleType setRoleType, List<String> defaultRoles) {
        this.userIdent = userDesc.getUserIdent();
        this.password = userDesc.getPassword();
        this.isPasswordPlain = userDesc.isPasswordPlain();
        this.authPlugin = userDesc.getAuthPlugin();
        this.authString = userDesc.getAuthString();

        this.setRoleType = setRoleType;
        this.defaultRoles = defaultRoles;
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


    public SetRoleType getSetRoleType() {
        return setRoleType;
    }

    public List<String> getDefaultRoles() {
        return defaultRoles;
    }

    public UserAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public void setAuthenticationInfo(UserAuthenticationInfo authenticationInfo) {
        this.authenticationInfo = authenticationInfo;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseCreateAlterUserStmt(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
