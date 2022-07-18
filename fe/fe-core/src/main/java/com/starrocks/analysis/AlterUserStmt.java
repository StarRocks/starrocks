// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

public class AlterUserStmt extends DdlStmt {

    private UserIdentity userIdent;
    private String password;
    private byte[] scramblePassword;
    private boolean isPasswordPlain;
    private String authPlugin;
    private String authString;
    private String userForAuthPlugin;

    public AlterUserStmt() {
    }

    public AlterUserStmt(UserDesc userDesc) {
        userIdent = userDesc.getUserIdent();
        password = userDesc.getPassword();
        isPasswordPlain = userDesc.isPasswordPlain();
        authPlugin = userDesc.getAuthPlugin();
        authString = userDesc.getAuthString();
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
                // plain password should check for validation & reuse
                Auth.validatePassword(password);
                GlobalStateMgr.getCurrentState().getAuth().checkPasswordReuse(userIdent, password);
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
            if (AuthPlugin.AUTHENTICATION_LDAP_SIMPLE.name().equals(authPlugin)) {
                this.userForAuthPlugin = this.authString;
            } else if (AuthPlugin.MYSQL_NATIVE_PASSWORD.name().equals(authPlugin)) {
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

        // check if current user has GRANT priv on GLOBAL or DATABASE level.
        if (!GlobalStateMgr.getCurrentState().getAuth()
                .checkHasPriv(ConnectContext.get(), PrivPredicate.GRANT, Auth.PrivLevel.GLOBAL,
                        Auth.PrivLevel.DATABASE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER USER ").append(userIdent);
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

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
