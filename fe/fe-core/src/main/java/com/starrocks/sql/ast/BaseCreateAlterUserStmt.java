// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;

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
