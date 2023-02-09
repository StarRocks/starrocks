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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.UserIdentity;

import java.util.ArrayList;
import java.util.List;

public class SetDefaultRoleStmt extends StatementBase {
    private enum SetRoleType {
        ALL,
        NONE,
        ROLE
    }

    private final UserIdentity userIdentity;
    private final List<String> roles = new ArrayList<>();
    private SetDefaultRoleStmt.SetRoleType setRoleType;

    public SetDefaultRoleStmt(UserIdentity userIdentity, List<String> roles) {
        this.userIdentity = userIdentity;
        this.roles.addAll(roles);
        this.setRoleType = SetDefaultRoleStmt.SetRoleType.ROLE;
    }

    public UserIdentity getUserIdentifier() {
        return userIdentity;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setTypeAll() {
        setRoleType = SetDefaultRoleStmt.SetRoleType.ALL;
    }

    public boolean isAll() {
        return setRoleType.equals(SetDefaultRoleStmt.SetRoleType.ALL);
    }

    public void setTypeNone() {
        setRoleType = SetDefaultRoleStmt.SetRoleType.NONE;
    }

    public boolean isNone() {
        return setRoleType.equals(SetDefaultRoleStmt.SetRoleType.NONE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetDefaultRoleStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
