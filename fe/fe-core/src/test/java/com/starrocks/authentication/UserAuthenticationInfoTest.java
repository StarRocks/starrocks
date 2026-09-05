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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UserAuthenticationInfoTest {

    @Test
    public void testUsernameExactMatch() throws Exception {
        UserAuthenticationInfo authInfo = new UserAuthenticationInfo();
        authInfo.setOrigUserHost("test_1", "%");

        // Should match exact username
        Assertions.assertTrue(authInfo.matchUser("test_1"));

        // Should NOT match variations to prevent authentication bypass
        Assertions.assertFalse(authInfo.matchUser("test.1"));
        Assertions.assertFalse(authInfo.matchUser("test=1"));
        Assertions.assertFalse(authInfo.matchUser("test*1"));
        Assertions.assertFalse(authInfo.matchUser("testa1"));
        Assertions.assertFalse(authInfo.matchUser("test 1"));
        Assertions.assertFalse(authInfo.matchUser("test1"));
        Assertions.assertFalse(authInfo.matchUser("test_11"));
        Assertions.assertFalse(authInfo.matchUser("test_"));
    }

    @Test
    public void testWildcardUser() throws Exception {
        UserAuthenticationInfo authInfo = new UserAuthenticationInfo();
        authInfo.setOrigUserHost("%", "%");

        // Wildcard user should match any username
        Assertions.assertTrue(authInfo.matchUser("test_1"));
        Assertions.assertTrue(authInfo.matchUser("test.1"));
        Assertions.assertTrue(authInfo.matchUser("anyuser"));
        Assertions.assertTrue(authInfo.matchUser("user_with_underscores"));
    }

    @Test
    public void testUsernameWithSpecialCharacters() throws Exception {
        String[] testUsernames = {
                "user.name@domain",
                "app_user_db_1",
                "service-account",
                "user$special"
        };

        for (String username : testUsernames) {
            UserAuthenticationInfo authInfo = new UserAuthenticationInfo();
            authInfo.setOrigUserHost(username, "%");

            Assertions.assertTrue(authInfo.matchUser(username));

            String modified = username.replace("_", ".").replace(".", "_");
            if (!modified.equals(username)) {
                Assertions.assertFalse(authInfo.matchUser(modified));
            }
        }
    }

    @Test
    public void testCaseSensitiveUsernames() throws Exception {
        UserAuthenticationInfo authInfo = new UserAuthenticationInfo();
        authInfo.setOrigUserHost("TestUser", "%");

        Assertions.assertTrue(authInfo.matchUser("TestUser"));

        Assertions.assertFalse(authInfo.matchUser("testuser"));
        Assertions.assertFalse(authInfo.matchUser("TESTUSER"));
    }
}
