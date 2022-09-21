// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataOutput;
import java.io.IOException;

public class UserAuthenticationInfo implements Writable {
    protected static final String ANY_HOST = "%";
    protected static final String ANY_USER = "%";

    @SerializedName(value = "p")
    private byte[] password = null;
    @SerializedName(value = "a")
    private String authPlugin = null;
    @SerializedName(value = "t")
    private String textForAuthPlugin = null;
    @SerializedName(value = "up")
    private UserProperty userProperty = new UserProperty();
    @SerializedName(value = "h")
    private String origHost;
    @SerializedName(value = "u")
    private String origUser;

    @Expose(serialize = false)
    private boolean isAnyUser;
    @Expose(serialize = false)
    private boolean isAnyHost;
    @Expose(serialize = false)
    protected PatternMatcher userPattern;
    @Expose(serialize = false)
    protected PatternMatcher hostPattern;

    public boolean match(String remoteUser, String remoteHost) {
        if (!isAnyUser && ! userPattern.match(remoteUser)) {
            return false;
        }
        return isAnyHost || hostPattern.match(remoteHost);
    }

    public void analyse() throws AuthenticationException {
        try {
            isAnyUser = origUser.equals(ANY_USER);
            isAnyHost = origHost.equals(ANY_HOST);
            userPattern = PatternMatcher.createMysqlPattern(origUser, CaseSensibility.USER.getCaseSensibility());
            hostPattern = PatternMatcher.createMysqlPattern(origHost, CaseSensibility.HOST.getCaseSensibility());
        } catch (AnalysisException e) {
            AuthenticationException exception = new AuthenticationException("failed to parse user host");
            exception.initCause(e);
            throw exception;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public byte[] getPassword() {
        return password;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public void setAuthPlugin(String authPlugin) {
        this.authPlugin = authPlugin;
    }

    public void setTextForAuthPlugin(String textForAuthPlugin) {
        this.textForAuthPlugin = textForAuthPlugin;
    }

    public void setOrigUserHost(String origUser, String origHost) throws AuthenticationException {
        this.origUser = origUser;
        this.origHost = origHost;
        analyse();
    }
}
