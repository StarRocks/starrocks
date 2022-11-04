// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserDesc;
import com.starrocks.mysql.privilege.Role;

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

    private boolean ifNotExist;

    public CreateUserStmt(UserDesc userDesc) {
        super(userDesc, "CREATE");
    }

    public CreateUserStmt(boolean ifNotExist, UserDesc userDesc, String role) {
        super(userDesc, role, "CREATE");
        this.ifNotExist = ifNotExist;
    }

    public boolean isIfNotExist() {
        return ifNotExist;
    }

    public boolean isSuperuser() {
        return role.equalsIgnoreCase(Role.ADMIN_ROLE);
    }
}
