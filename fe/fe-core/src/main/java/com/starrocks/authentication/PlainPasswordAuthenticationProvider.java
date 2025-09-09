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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorCode;
import com.starrocks.mysql.MysqlPassword;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class PlainPasswordAuthenticationProvider implements AuthenticationProvider {
    private final byte[] password;

    public PlainPasswordAuthenticationProvider(byte[] password) {
        this.password = password;
    }

    @Override
    public void authenticate(
            AuthenticationContext authContext,
            UserIdentity userIdentity,
            byte[] authResponse) throws AuthenticationException {
        String usePassword = authResponse.length == 0 ? "NO" : "YES";
        byte[] randomString = authContext.getAuthDataSalt();
        // The password sent by mysql client has already been scrambled(encrypted) using random string,
        // so we don't need to scramble it again.
        if (randomString != null) {
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(password);
            if (saltPassword.length != authResponse.length) {
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }

            if (authResponse.length > 0 && !MysqlPassword.checkScramble(authResponse, randomString, saltPassword)) {
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }
        } else {
            // Plain remote password, scramble it first.
            byte[] scrambledRemotePass = MysqlPassword.makeScrambledPassword((StringUtils.stripEnd(
                    new String(authResponse, StandardCharsets.UTF_8), "\0")));
            if (!MysqlPassword.checkScrambledPlainPass(password, scrambledRemotePass)) {
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }
        }
    }

    @Override
    public byte[] authSwitchRequestPacket(AuthenticationContext authContext, String user, String host)
            throws AuthenticationException {
        return authContext.getAuthDataSalt();
    }
}
