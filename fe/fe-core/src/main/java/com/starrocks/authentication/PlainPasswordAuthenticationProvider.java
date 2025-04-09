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
import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PlainPasswordAuthenticationProvider implements AuthenticationProvider {
    /**
     * check password complexity if `enable_validate_password` is set
     * <p>
     * The rules are hard-coded for temporary, will change to a plugin config later
     **/
    protected void validatePassword(UserIdentity userIdentity, String password) throws AuthenticationException {
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

        if (!Config.enable_password_reuse) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            Map.Entry<UserIdentity, UserAuthenticationInfo> userAuthenticationInfoEntry =
                    authenticationMgr.getBestMatchedUserIdentity(userIdentity.getUser(), userIdentity.getHost());
            if (userAuthenticationInfoEntry != null) {
                try {
                    authenticate(null, userIdentity.getUser(), userIdentity.getHost(),
                            password.getBytes(StandardCharsets.UTF_8), null, userAuthenticationInfoEntry.getValue());
                } catch (AuthenticationException e) {
                    return;
                }

                throw new AuthenticationException("Can't reuse password");
            }
        }
    }

    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        byte[] passwordScrambled = MysqlPassword.EMPTY_PASSWORD;
        if (userAuthOption != null) {
            boolean isPasswordPlain = userAuthOption.isPasswordPlain();
            String password = userAuthOption.getAuthPlugin() == null ?
                    userAuthOption.getPassword() : userAuthOption.getAuthString();
            if (isPasswordPlain) {
                validatePassword(userIdentity, password);
            }
            passwordScrambled = scramblePassword(password, isPasswordPlain);
        }

        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setAuthPlugin(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name());
        info.setPassword(passwordScrambled);
        info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        info.setAuthString(userAuthOption == null ? null : userAuthOption.getAuthString());
        return info;
    }

    @Override
    public void authenticate(
            ConnectContext context,
            String user,
            String host,
            byte[] remotePassword,
            byte[] randomString,
            UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        // The password sent by mysql client has already been scrambled(encrypted) using random string,
        // so we don't need to scramble it again.
        if (randomString != null) {
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(authenticationInfo.getPassword());
            if (saltPassword.length != remotePassword.length) {
                throw new AuthenticationException("password length mismatch!");
            }

            if (remotePassword.length > 0
                    && !MysqlPassword.checkScramble(remotePassword, randomString, saltPassword)) {
                throw new AuthenticationException("password mismatch!");
            }
        } else {
            // Plain remote password, scramble it first.
            byte[] scrambledRemotePass =
                    MysqlPassword.makeScrambledPassword((StringUtils.stripEnd(
                            new String(remotePassword, StandardCharsets.UTF_8), "\0")));
            if (!MysqlPassword.checkScrambledPlainPass(authenticationInfo.getPassword(), scrambledRemotePass)) {
                throw new AuthenticationException("password mismatch!");
            }
        }
    }

    /**
     * Get scrambled password from plain password
     */
    private byte[] scramblePassword(String originalPassword, boolean isPasswordPlain) {
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
    public byte[] authSwitchRequestPacket(String user, String host, byte[] randomString) throws AuthenticationException {
        return randomString;
    }
}
