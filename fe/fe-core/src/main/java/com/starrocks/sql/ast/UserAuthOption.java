// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;

public class UserAuthOption implements ParseNode {
    private final String password;
    private final String authPlugin;
    private final String authString;
    private final boolean passwordPlain;

    public UserAuthOption(String password, String authPlugin, String authString, boolean passwordPlain) {
        this.password = password;
        this.authPlugin = authPlugin;
        this.authString = authString;
        this.passwordPlain = passwordPlain;
    }

    public String getPassword() {
        return password;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public String getAuthString() {
        return authString;
    }

    public boolean isPasswordPlain() {
        return passwordPlain;
    }
}
