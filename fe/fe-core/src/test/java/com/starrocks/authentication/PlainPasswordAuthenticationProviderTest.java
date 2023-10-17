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
import com.starrocks.sql.ast.UserIdentity;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static com.starrocks.mysql.MysqlPassword.EMPTY_PASSWORD;

public class PlainPasswordAuthenticationProviderTest {
    protected PlainPasswordAuthenticationProvider provider = new PlainPasswordAuthenticationProvider();

    @Test
    public void testValidPassword() throws Exception {
        Config.enable_validate_password = true;

        // too short
        try {
            provider.validatePassword("aaa");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password is too short"));
        }

        // only number
        String[] badPasswords = {"starrocks", "STARROCKS", "123456789", "STARROCKS123", "starrocks123", "STARROCKSstar"};
        for (String badPassword : badPasswords) {
            try {
                provider.validatePassword(badPassword);
                Assert.fail();
            } catch (AuthenticationException e) {
                Assert.assertTrue(e.getMessage().contains(
                        "password should contains at least one digit, one lowercase letter and one uppercase letter!"));
            }
        }

        provider.validatePassword("aaaAAA123");
        Config.enable_validate_password = false;
        provider.validatePassword("aaa");
    }

    @Test
    public void testAuthentication() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        String[] passwords = {"asdf123", "starrocks", "testtest"};
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        for (String password : passwords) {
            UserAuthenticationInfo info = provider.validAuthenticationInfo(testUser,
                    new String(MysqlPassword.makeScrambledPassword(password), StandardCharsets.UTF_8), null);
            byte[] scramble = MysqlPassword.scramble(seed, password);
            provider.authenticate(testUser.getUser(), "10.1.1.1", scramble, seed, info);
        }

        // no password
        UserAuthenticationInfo info = provider.validAuthenticationInfo(testUser,
                new String(EMPTY_PASSWORD, StandardCharsets.UTF_8), null);
        provider.authenticate(testUser.getUser(), "10.1.1.1", new byte[0], new byte[0], info);
        try {
            provider.authenticate(
                    testUser.getUser(),
                    "10.1.1.1",
                    "xx".getBytes(StandardCharsets.UTF_8),
                    "x".getBytes(StandardCharsets.UTF_8),
                    info);
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password length mismatch!"));
        }

        byte[] p = MysqlPassword.makeScrambledPassword("bb");
        info = provider.validAuthenticationInfo(testUser, new String(p, StandardCharsets.UTF_8), null);
        try {
            provider.authenticate(
                    testUser.getUser(),
                    "10.1.1.1",
                    MysqlPassword.scramble(seed, "xx"),
                    seed,
                    info);
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password mismatch!"));
        }

        try {
            provider.authenticate(
                    testUser.getUser(),
                    "10.1.1.1",
                    MysqlPassword.scramble(seed, "bb"),
                    seed,
                    info);

        } catch (AuthenticationException e) {
            Assert.fail();
        }

        try {
            byte[] remotePassword = "bb".getBytes(StandardCharsets.UTF_8);
            provider.authenticate(
                    testUser.getUser(),
                    "10.1.1.1",
                    remotePassword,
                    null,
                    info);

        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }
}
