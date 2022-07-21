// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.sql.ast.AstVisitor;

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

    @Override
    public boolean needAuditEncryption() {
        return true;
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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterUserStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
