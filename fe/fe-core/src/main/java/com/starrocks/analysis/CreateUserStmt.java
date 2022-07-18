// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateUserStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.Auth.PrivLevel;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Role;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class CreateUserStmt extends DdlStmt {

    private boolean ifNotExist;
    private UserIdentity userIdent;
    private String password;
    private byte[] scramblePassword;
    private boolean isPasswordPlain;
    private String authPlugin;
    private String authString;
    private String userForAuthPlugin;
    private String role;

    public CreateUserStmt() {
    }

    public CreateUserStmt(UserDesc userDesc) {
        userIdent = userDesc.getUserIdent();
        password = userDesc.getPassword();
        isPasswordPlain = userDesc.isPasswordPlain();
        authPlugin = userDesc.getAuthPlugin();
        authString = userDesc.getAuthString();
    }

    public CreateUserStmt(boolean ifNotExist, UserDesc userDesc, String role) {
        this.ifNotExist = ifNotExist;
        this.role = role;
        userIdent = userDesc.getUserIdent();
        password = userDesc.getPassword();
        isPasswordPlain = userDesc.isPasswordPlain();
        authPlugin = userDesc.getAuthPlugin();
        authString = userDesc.getAuthString();
    }

    public boolean isIfNotExist() {
        return ifNotExist;
    }

    public boolean isSuperuser() {
        return role.equalsIgnoreCase(Role.ADMIN_ROLE);
    }

    public boolean hasRole() {
        return role != null;
    }

    public String getQualifiedRole() {
        return role;
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

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        userIdent.analyze();

        /*
         * IDENTIFIED BY
         */
        // convert password to hashed password
        if (!Strings.isNullOrEmpty(password)) {
            if (isPasswordPlain) {
                // plain password validation
                Auth.validatePassword(password);
                // convert plain password to scramble
                scramblePassword = MysqlPassword.makeScrambledPassword(password);
            } else {
                scramblePassword = MysqlPassword.checkPassword(password);
            }
        } else {
            scramblePassword = new byte[0];
        }

        /*
         * IDENTIFIED WITH
         */
        if (!Strings.isNullOrEmpty(authPlugin)) {
            authPlugin = authPlugin.toUpperCase();
            if (AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name().equalsIgnoreCase(authPlugin)) {
                this.userForAuthPlugin = this.authString;
            } else if (AuthPlugin.MYSQL_NATIVE_PASSWORD.name().equalsIgnoreCase(authPlugin)) {
                // in this case, authString is password
                // convert password to hashed password
                if (!Strings.isNullOrEmpty(authString)) {
                    if (isPasswordPlain) {
                        // convert plain password to scramble
                        scramblePassword = MysqlPassword.makeScrambledPassword(authString);
                    } else {
                        scramblePassword = MysqlPassword.checkPassword(authString);
                    }
                } else {
                    scramblePassword = new byte[0];
                }
            } else if (AuthPlugin.AUTHENTICATION_KERBEROS.name().equalsIgnoreCase(authPlugin) &&
                    GlobalStateMgr.getCurrentState().getAuth().isSupportKerberosAuth()) {
                // In kerberos authentication, userForAuthPlugin represents the user principal realm.
                // If user realm is not specified when creating user, the service principal realm will be used as
                // the user principal realm by default.
                if (authString != null) {
                    userForAuthPlugin = this.authString;
                } else {
                    userForAuthPlugin = Config.authentication_kerberos_service_principal.split("@")[1];
                }
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_AUTH_PLUGIN_NOT_LOADED, authPlugin);
            }
        }

        if (role != null) {
            if (role.equalsIgnoreCase("SUPERUSER")) {
                // for forward compatibility
                role = Role.ADMIN_ROLE;
            }
            FeNameFormat.checkRoleName(role, true /* can be admin */, "Can not granted user to role");
            role = ClusterNamespace.getFullName(role);
        }

        // check if current user has GRANT priv on GLOBAL or DATABASE level.
        if (!GlobalStateMgr.getCurrentState().getAuth()
                .checkHasPriv(ConnectContext.get(), PrivPredicate.GRANT, PrivLevel.GLOBAL, PrivLevel.DATABASE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE USER ").append(userIdent);
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
}
