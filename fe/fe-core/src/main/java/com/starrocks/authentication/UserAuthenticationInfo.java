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

package com.starrocks.authentication;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserLockOption;
import com.starrocks.sql.ast.UserPasswordOption;
import com.starrocks.sql.ast.UserRef;

import java.io.IOException;

public class UserAuthenticationInfo implements Writable, GsonPostProcessable {
    protected static final String ANY_HOST = "%";
    protected static final String ANY_USER = "%";

    @SerializedName(value = "p")
    private byte[] password;
    @SerializedName(value = "a")
    private String authPlugin = null;
    @SerializedName(value = "t")
    private String authString = null;
    @SerializedName(value = "h")
    private String origHost;
    @SerializedName(value = "u")
    private String origUser;

    @SerializedName(value = "pe")
    private boolean passwordExpired = false;
    @SerializedName(value = "plm")
    private long passwordLastModifiedTimestamp = System.currentTimeMillis();

    @SerializedName(value = "lock")
    private boolean lock;
    @SerializedName(value = "freezeTs")
    private long lockTimestamp;
    private int errorPasswordRetries = 0;

    private boolean isAnyUser;
    private boolean isAnyHost;
    protected PatternMatcher userPattern;
    protected PatternMatcher hostPattern;

    public UserAuthenticationInfo(UserAuthenticationInfo info) {
        this.password = info.password;
        this.authPlugin = info.authPlugin;
        this.authString = info.authString;
        this.origHost = info.origHost;
        this.origUser = info.origUser;
        this.passwordExpired = info.passwordExpired;
        this.passwordLastModifiedTimestamp = info.passwordLastModifiedTimestamp;
        this.lock = info.lock;
        this.lockTimestamp = info.lockTimestamp;
        this.errorPasswordRetries = info.errorPasswordRetries;
        loadMysqlPattern();
    }

    public UserAuthenticationInfo(UserRef user, UserAuthOption userAuthOption) {
        this(user, userAuthOption, null, null);
    }

    public UserAuthenticationInfo(UserRef user,
                                  UserAuthOption userAuthOption,
                                  UserPasswordOption userPasswordOption,
                                  UserLockOption userLockOption) {
        String authPluginUsing;
        if (userAuthOption == null || userAuthOption.getAuthPlugin() == null) {
            authPluginUsing = AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString();
        } else {
            authPluginUsing = userAuthOption.getAuthPlugin();
        }

        this.authPlugin = authPluginUsing;
        
        if (authPluginUsing.equalsIgnoreCase(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString())) {
            byte[] passwordScrambled = MysqlPassword.EMPTY_PASSWORD;
            if (userAuthOption != null) {
                passwordScrambled = scramblePassword(userAuthOption.getAuthString(), userAuthOption.isPasswordPlain());
            }
            this.password = passwordScrambled;
            this.authString = null;
        } else {
            this.password = MysqlPassword.EMPTY_PASSWORD;
            this.authString = userAuthOption == null ? null : userAuthOption.getAuthString();
        }

        if (userPasswordOption != null) {
            this.passwordExpired = userPasswordOption.isExpirePassword();
        }
        this.passwordLastModifiedTimestamp = System.currentTimeMillis();

        if (userLockOption != null) {
            this.lock = userLockOption.isLock();
        }

        this.origUser = user.getUser();
        this.origHost = user.getHost();

        loadMysqlPattern();
    }

    public boolean matchUser(String remoteUser) {
        return isAnyUser || userPattern.match(remoteUser);
    }

    public boolean matchHost(String remoteHost) {
        return isAnyHost || hostPattern.match(remoteHost);
    }

    private void loadMysqlPattern() {
        isAnyUser = origUser.equals(ANY_USER);
        isAnyHost = origHost.equals(ANY_HOST);
        userPattern = PatternMatcher.createMysqlPattern(origUser, CaseSensibility.USER.getCaseSensibility());
        hostPattern = PatternMatcher.createMysqlPattern(origHost, CaseSensibility.HOST.getCaseSensibility());
    }

    public byte[] getPassword() {
        return password;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public String getOrigHost() {
        return origHost;
    }

    public String getOrigUser() {
        return origUser;
    }

    public String getAuthString() {
        return authString;
    }

    /**
     * Get scrambled password from plain password
     */
    private static byte[] scramblePassword(String originalPassword, boolean isPasswordPlain) {
        if (Strings.isNullOrEmpty(originalPassword)) {
            return MysqlPassword.EMPTY_PASSWORD;
        }
        if (isPasswordPlain) {
            return MysqlPassword.makeScrambledPassword(originalPassword);
        } else {
            return MysqlPassword.checkPassword(originalPassword);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        loadMysqlPattern();
    }

    public void setPasswordExpired(boolean passwordExpired) {
        this.passwordExpired = passwordExpired;
    }

    public boolean isPasswordExpired() {
        return passwordExpired;
    }

    public void setPasswordLastModifiedTimestamp(long passwordLastModifiedTimestamp) {
        this.passwordLastModifiedTimestamp = passwordLastModifiedTimestamp;
    }

    public long getPasswordLastModifiedTimestamp() {
        return passwordLastModifiedTimestamp;
    }

    public void setLock(boolean lock) {
        this.lock = lock;
    }

    public boolean isLock() {
        return lock;
    }

    public int getErrorPasswordRetries() {
        return errorPasswordRetries;
    }

    public void increasePasswordErrorTimes() {
        errorPasswordRetries++;
    }

    public void clearPasswordErrorTimes() {
        errorPasswordRetries = 0;
    }

    public void setLockTimestamp(long lockTimestamp) {
        this.lockTimestamp = lockTimestamp;
    }

    public long getLockTimestamp() {
        return lockTimestamp;
    }
}
