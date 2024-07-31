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

import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.sql.parser.NodePosition;

// CreateUserStmt and AlterUserStmt share the same parameter and check logic
public abstract class BaseCreateAlterUserStmt extends DdlStmt {
    protected UserIdentity userIdentity;
    protected UserAuthOption authOption;

    // used in new RBAC privilege framework
    private UserAuthenticationInfo authenticationInfo = null;

    public BaseCreateAlterUserStmt(UserIdentity userIdentity, UserAuthOption authOption, NodePosition pos) {
        super(pos);

        this.userIdentity = userIdentity;
        this.authOption = authOption;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public UserAuthOption getAuthOption() {
        return authOption;
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
}
