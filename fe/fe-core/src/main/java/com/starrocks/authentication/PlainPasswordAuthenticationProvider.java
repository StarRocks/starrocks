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

import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.sql.ast.UserIdentity;

import java.nio.charset.StandardCharsets;

public class PlainPasswordAuthenticationProvider implements AuthenticationProvider {
    public static final String PLUGIN_NAME = "MYSQL_NATIVE_PASSWORD";

    /**
     * check password complexity if `enable_validate_password` is set
     * <p>
     * The rules are hard-coded for temporary, will change to a plugin config later
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
        info.setPassword(password.getBytes(StandardCharsets.UTF_8));
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
                    MysqlPassword.makeScrambledPassword(new String(remotePassword, StandardCharsets.UTF_8));
            if (!MysqlPassword.checkScrambledPlainPass(authenticationInfo.getPassword(), scrambledRemotePass)) {
                throw new AuthenticationException("password mismatch!");
            }
        }
    }

    @Override
    public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException {
        UserAuthenticationInfo ret = new UserAuthenticationInfo();
        ret.setPassword(password.getPassword() == null ? MysqlPassword.EMPTY_PASSWORD : password.getPassword());
        ret.setAuthPlugin(PLUGIN_NAME);
        ret.setOrigUserHost(userIdentity.getQualifiedUser(), userIdentity.getHost());
        ret.setTextForAuthPlugin(password.getUserForAuthPlugin());
        return ret;
    }
}
