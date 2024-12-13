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

<<<<<<< HEAD
import com.starrocks.analysis.UserDesc;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// CreateUserStmt and AlterUserStmt share the same parameter and check logic
public class BaseCreateAlterUserStmt extends DdlStmt {
    protected UserIdentity userIdentity;
    protected String password;
    protected boolean isPasswordPlain;
    protected String authPluginName;
    protected String authStringUnResolved;

    protected SetRoleType setRoleType;
    protected List<String> defaultRoles;
    // used in new RBAC privilege framework
    private UserAuthenticationInfo authenticationInfo = null;

    @Deprecated
    protected String userForAuthPlugin;
    @Deprecated
    protected byte[] scramblePassword;

    public BaseCreateAlterUserStmt(UserDesc userDesc, SetRoleType setRoleType, List<String> defaultRoles) {
        this(userDesc, setRoleType, defaultRoles, NodePosition.ZERO);
    }

    public BaseCreateAlterUserStmt(UserDesc userDesc, SetRoleType setRoleType, List<String> defaultRoles,
                                   NodePosition pos) {
        super(pos);
        this.userIdentity = userDesc.getUserIdentity();
        this.password = userDesc.getPassword();
        this.isPasswordPlain = userDesc.isPasswordPlain();
        this.authPluginName = userDesc.getAuthPlugin();
        this.authStringUnResolved = userDesc.getAuthString();

        this.setRoleType = setRoleType;
        this.defaultRoles = defaultRoles;
=======
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// CreateUserStmt and AlterUserStmt share the same parameter and check logic
public abstract class BaseCreateAlterUserStmt extends DdlStmt {
    protected UserIdentity userIdentity;
    protected UserAuthOption authOption;

    // used in new RBAC privilege framework
    private UserAuthenticationInfo authenticationInfo = null;

    private final Map<String, String> properties;

    public BaseCreateAlterUserStmt(UserIdentity userIdentity, UserAuthOption authOption,
                                   Map<String, String> properties, NodePosition pos) {
        super(pos);

        this.userIdentity = userIdentity;
        this.authOption = authOption;
        this.properties = properties;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

<<<<<<< HEAD
    public String getOriginalPassword() {
        return password;
    }

    public boolean isPasswordPlain() {
        return isPasswordPlain;
    }

    public String getAuthPluginName() {
        return authPluginName;
    }

    public String getAuthStringUnResolved() {
        return authStringUnResolved;
    }

    public List<String> getDefaultRoles() {
        return defaultRoles;
=======
    public UserAuthOption getAuthOption() {
        return authOption;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public UserAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public void setAuthenticationInfo(UserAuthenticationInfo authenticationInfo) {
        this.authenticationInfo = authenticationInfo;
    }

<<<<<<< HEAD
=======
    public Map<String, String> getProperties() {
        return properties;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseCreateAlterUserStmt(this, context);
    }
<<<<<<< HEAD

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Deprecated
    public void setScramblePassword(byte[] scramblePassword) {
        this.scramblePassword = scramblePassword;
    }

    @Deprecated
    public byte[] getPassword() {
        return scramblePassword;
    }

    @Deprecated
    public String getUserForAuthPlugin() {
        return userForAuthPlugin;
    }

    @Deprecated
    public void setUserForAuthPlugin(String userForAuthPlugin) {
        this.userForAuthPlugin = userForAuthPlugin;
    }
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
