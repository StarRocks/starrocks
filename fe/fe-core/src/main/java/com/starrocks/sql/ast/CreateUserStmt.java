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

import java.util.List;

/*
 * We support the following create user stmts:
 * 1. create user user@'ip' [identified by 'password' | identified with auth_plugin [AS | BY auth_string]]
 *      specify the user name at a certain ip(wildcard is accepted), with optional password or auth plugin.
 *      the user@ip must not exist in system
 *
 * 2. create user user@['domain'] [identified by 'password' | identified with auth_plugin [AS | BY auth_string]]
 *      specify the user name at a certain domain, with optional password or auth plugin.
 *      the user@['domain'] must not exist in system
 *      the daemon thread will resolve this domain to user@'ip' format
 *
 * 3. create user user@xx [identified by 'password' | identified with auth_plugin [AS | BY auth_string]] role role_name
 *      not only create the specified user, but also grant all privs of the specified role to the user.
 */
public class CreateUserStmt extends BaseCreateAlterUserStmt {

    private final boolean ifNotExist;

    public CreateUserStmt(boolean ifNotExist, UserDesc userDesc, List<String> defaultRoles) {
        super(userDesc, SetRoleType.ROLE, defaultRoles);
        this.ifNotExist = ifNotExist;
    }

    public boolean isIfNotExist() {
        return ifNotExist;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateUserStatement(this, context);
    }
}
