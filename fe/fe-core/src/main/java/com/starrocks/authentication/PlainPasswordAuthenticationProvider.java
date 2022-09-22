// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlPassword;

public class PlainPasswordAuthenticationProvider implements AuthenticationProvider {
    public static final String PLUGIN_NAME = "MYSQL_NATIVE_PASSWORD";

    /**
     * check password complexity if `enable_validate_password` is set
     *
     * The rules is hard-coded for temporary, will change to a plugin config later
     **/
    protected void validatePassword(String password) throws AuthenticationException {
        if (!Config.enable_validate_password) {
            return;
        }

        //  1. The length of the password should be no less than 8.
        if (password.length() < 8) {
            throw new AuthenticationException("password is too short!");
        }

        // 2. The password should contain at least one digit, one lowercase letter, one uppercase letter
        boolean hasDigit = false;
        boolean hasUpper = false;
        boolean hasLower = false;
        for (int i = 0; i != password.length(); ++i) {
            char c = password.charAt(i);
            if (c >= '0' && c <= '9') {
                hasDigit = true;
            } else if (c >= 'A' && c <= 'Z') {
                hasUpper = true;
            } else if (c >= 'a' && c <= 'z') {
                hasLower = true;
            }
        }
        if (!hasDigit || !hasLower || !hasUpper) {
            throw new AuthenticationException(
                    "password should contains at least one digit, one lowercase letter and one uppercase letter!");
        }
    }

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(
            UserIdentity userIdentity,
            String password,
            String textForAuthPlugin) throws AuthenticationException {
        validatePassword(password);

        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setPassword(MysqlPassword.makeScrambledPassword(password));
        info.setTextForAuthPlugin(textForAuthPlugin);
        return info;
    }

    @Override
    public void authenticate(
            String user,
            String host,
            byte[] remotePassword,
            byte[] randomString,
            UserAuthenticationInfo authenticationInfo) throws AuthenticationException {

        byte[] saltPassword = MysqlPassword.getSaltFromPassword(authenticationInfo.getPassword());
        if (saltPassword.length != remotePassword.length) {
            throw new AuthenticationException("password length mismatch!");
        }

        if (remotePassword.length > 0
                && !MysqlPassword.checkScramble(remotePassword, randomString, saltPassword)) {
            throw new AuthenticationException("password mismatch!");
        }
    }
}
