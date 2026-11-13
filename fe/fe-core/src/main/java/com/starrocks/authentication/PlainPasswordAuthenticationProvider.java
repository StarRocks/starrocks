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

import java.nio.charset.StandardCharsets;

public class PlainPasswordAuthenticationProvider implements AuthenticationProvider {
    private final byte[] password;

    public PlainPasswordAuthenticationProvider(byte[] password) {
        this.password = password;
    }

    @Override
    public UserIdentity authenticate(
            AccessControlContext authContext,
            UserIdentity userIdentity,
            AuthCredentials cred) throws AuthenticationException {
        // Accept-list rather than exclusions, so a kind added later is refused by default:
        //   MYSQL_NATIVE_SCRAMBLE - scrambled against the salt the server issued in its greeting
        //   PLAIN_PASSWORD        - the plaintext password from HTTP Basic and friends
        // Which check runs below is decided by the salt, not by the kind: a scramble is only
        // checkable against the salt that produced it, and a context that never went through the
        // greeting has none - in which case the bytes can only be compared as a password.
        byte[] authResponse;
        switch (cred.kind()) {
            case MYSQL_NATIVE_SCRAMBLE, PLAIN_PASSWORD -> authResponse = cred.rawSecret();
            default -> throw new AuthenticationException(
                    "password user cannot authenticate with " + cred.kind());
        }
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
            // Plain remote password, scramble it first. The bytes are the password and nothing
            // else: the MySQL entry point already dropped the frame's trailing NUL, and no other
            // channel adds one.
            byte[] scrambledRemotePass = MysqlPassword.makeScrambledPassword(
                    new String(authResponse, StandardCharsets.UTF_8));
            if (!MysqlPassword.checkScrambledPlainPass(password, scrambledRemotePass)) {
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, userIdentity.getUser(), usePassword);
            }
        }
        return userIdentity;
    }

    @Override
    public boolean supportsCredentialKind(AuthCredentialKind kind) {
        // Enumerated rather than asked as "is this a password": a MySQL frame may hold something
        // else entirely, so the question only makes sense per provider.
        return kind == AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE || kind == AuthCredentialKind.PLAIN_PASSWORD;
    }

    @Override
    public byte[] authSwitchRequestPacket(AccessControlContext authContext, String user, String host)
            throws AuthenticationException {
        return authContext.getAuthDataSalt();
    }
}
