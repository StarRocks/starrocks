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

import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserLockOption;
import com.starrocks.sql.ast.UserPasswordOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UserAuthenticationInfoTest {

    @Test
    public void testCloneConstructorConsistency() {
        // Create original UserAuthenticationInfo with all fields set
        UserRef user = new UserRef("testuser", "localhost");
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString(),
                "password123",
                true,
                NodePosition.ZERO
        );
        UserPasswordOption passwordOption = new UserPasswordOption(true);
        UserLockOption lockOption = new UserLockOption(true);

        UserAuthenticationInfo original = new UserAuthenticationInfo(
                user, authOption, passwordOption, lockOption
        );

        // Set additional fields that can be modified
        original.setPasswordLastModifiedTimestamp(1234567890L);
        original.setLockTimestamp(9876543210L);
        original.increasePasswordErrorTimes();
        original.increasePasswordErrorTimes();

        // Create clone using copy constructor
        UserAuthenticationInfo cloned = new UserAuthenticationInfo(original);

        // Verify all fields are consistent
        Assertions.assertArrayEquals(original.getPassword(), cloned.getPassword(),
                "Password should be equal");
        Assertions.assertEquals(original.getAuthPlugin(), cloned.getAuthPlugin(),
                "AuthPlugin should be equal");
        Assertions.assertEquals(original.getAuthString(), cloned.getAuthString(),
                "AuthString should be equal");
        Assertions.assertEquals(original.getOrigHost(), cloned.getOrigHost(),
                "OrigHost should be equal");
        Assertions.assertEquals(original.getOrigUser(), cloned.getOrigUser(),
                "OrigUser should be equal");
        Assertions.assertEquals(original.isPasswordExpired(), cloned.isPasswordExpired(),
                "PasswordExpired should be equal");
        Assertions.assertEquals(original.getPasswordLastModifiedTimestamp(),
                cloned.getPasswordLastModifiedTimestamp(),
                "PasswordLastModifiedTimestamp should be equal");
        Assertions.assertEquals(original.isLock(), cloned.isLock(),
                "Lock should be equal");
        Assertions.assertEquals(original.getLockTimestamp(), cloned.getLockTimestamp(),
                "LockTimestamp should be equal");
        Assertions.assertEquals(original.getErrorPasswordRetries(), cloned.getErrorPasswordRetries(),
                "ErrorPasswordRetries should be equal");

        // Verify pattern matching works correctly
        Assertions.assertEquals(original.matchUser("testuser"), cloned.matchUser("testuser"),
                "User matching should be equal");
        Assertions.assertEquals(original.matchHost("localhost"), cloned.matchHost("localhost"),
                "Host matching should be equal");
    }

    @Test
    public void testCloneModificationDoesNotAffectOriginal() {
        // Create original UserAuthenticationInfo
        UserRef user = new UserRef("testuser", "%");
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString(),
                "originalPassword",
                true,
                NodePosition.ZERO
        );
        UserPasswordOption passwordOption = new UserPasswordOption(false);
        UserLockOption lockOption = new UserLockOption(false);

        UserAuthenticationInfo original = new UserAuthenticationInfo(
                user, authOption, passwordOption, lockOption
        );
        original.setPasswordLastModifiedTimestamp(1000L);
        original.setLockTimestamp(2000L);
        original.increasePasswordErrorTimes();

        // Store original values
        byte[] originalPassword = original.getPassword();
        String originalAuthPlugin = original.getAuthPlugin();
        String originalAuthString = original.getAuthString();
        String originalOrigHost = original.getOrigHost();
        String originalOrigUser = original.getOrigUser();
        boolean originalPasswordExpired = original.isPasswordExpired();
        long originalPasswordLastModifiedTimestamp = original.getPasswordLastModifiedTimestamp();
        boolean originalLock = original.isLock();
        long originalLockTimestamp = original.getLockTimestamp();
        int originalErrorPasswordRetries = original.getErrorPasswordRetries();

        // Create clone
        UserAuthenticationInfo cloned = new UserAuthenticationInfo(original);

        // Modify clone's fields
        cloned.setPasswordExpired(true);
        cloned.setPasswordLastModifiedTimestamp(9999L);
        cloned.setLock(true);
        cloned.setLockTimestamp(8888L);
        cloned.increasePasswordErrorTimes();
        cloned.increasePasswordErrorTimes();
        cloned.clearPasswordErrorTimes();

        // Verify original values are unchanged
        Assertions.assertArrayEquals(originalPassword, original.getPassword(),
                "Original password should not be affected");
        Assertions.assertEquals(originalAuthPlugin, original.getAuthPlugin(),
                "Original authPlugin should not be affected");
        Assertions.assertEquals(originalAuthString, original.getAuthString(),
                "Original authString should not be affected");
        Assertions.assertEquals(originalOrigHost, original.getOrigHost(),
                "Original origHost should not be affected");
        Assertions.assertEquals(originalOrigUser, original.getOrigUser(),
                "Original origUser should not be affected");
        Assertions.assertEquals(originalPasswordExpired, original.isPasswordExpired(),
                "Original passwordExpired should not be affected");
        Assertions.assertEquals(originalPasswordLastModifiedTimestamp,
                original.getPasswordLastModifiedTimestamp(),
                "Original passwordLastModifiedTimestamp should not be affected");
        Assertions.assertEquals(originalLock, original.isLock(),
                "Original lock should not be affected");
        Assertions.assertEquals(originalLockTimestamp, original.getLockTimestamp(),
                "Original lockTimestamp should not be affected");
        Assertions.assertEquals(originalErrorPasswordRetries, original.getErrorPasswordRetries(),
                "Original errorPasswordRetries should not be affected");
    }

    @Test
    public void testClonePasswordArrayIndependence() {
        // Test password array behavior in clone
        // Note: In the current implementation, password is a reference copy (shallow copy),
        // so modifying the array content would affect both original and clone.
        // This test documents this behavior and verifies that other fields are independent.
        UserRef user = new UserRef("testuser", "localhost");
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString(),
                "testPassword",
                true,
                NodePosition.ZERO
        );

        UserAuthenticationInfo original = new UserAuthenticationInfo(user, authOption);
        byte[] originalPassword = original.getPassword();

        // Create clone
        UserAuthenticationInfo cloned = new UserAuthenticationInfo(original);
        byte[] clonedPassword = cloned.getPassword();

        // Verify passwords are equal initially
        Assertions.assertArrayEquals(originalPassword, clonedPassword,
                "Clone password should equal original password initially");

        // Test that modifying the password array content affects both (shallow copy behavior)
        // This documents the current implementation behavior
        if (clonedPassword.length > 0) {
            byte originalFirstByte = originalPassword[0];
            clonedPassword[0] = (byte) (clonedPassword[0] ^ 0xFF);
            
            // Verify that original password array is also affected (shallow copy)
            Assertions.assertEquals(clonedPassword[0], originalPassword[0],
                    "Password arrays share the same reference (shallow copy behavior)");
            
            // Restore for other tests
            clonedPassword[0] = originalFirstByte;
        }

        // Verify that other setter methods work independently
        original.setPasswordExpired(true);
        cloned.setPasswordExpired(false);
        Assertions.assertTrue(original.isPasswordExpired(),
                "Original passwordExpired should be true");
        Assertions.assertFalse(cloned.isPasswordExpired(),
                "Clone passwordExpired should be false");
        
        // Reset
        original.setPasswordExpired(false);
        cloned.setPasswordExpired(false);
    }

    @Test
    public void testCloneWithNullFields() {
        // Test clone with minimal fields (null auth option)
        UserRef user = new UserRef("testuser", "%");
        UserAuthenticationInfo original = new UserAuthenticationInfo(user, null);
        original.setPasswordExpired(true);
        original.setLock(true);

        UserAuthenticationInfo cloned = new UserAuthenticationInfo(original);

        Assertions.assertEquals(original.getAuthPlugin(), cloned.getAuthPlugin());
        Assertions.assertEquals(original.getAuthString(), cloned.getAuthString());
        Assertions.assertEquals(original.isPasswordExpired(), cloned.isPasswordExpired());
        Assertions.assertEquals(original.isLock(), cloned.isLock());
    }

    @Test
    public void testCloneWithNonNativeAuthPlugin() {
        // Test clone with non-native auth plugin (e.g., LDAP)
        UserRef user = new UserRef("testuser", "localhost");
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.toString(),
                "ldap_auth_string",
                false,
                NodePosition.ZERO
        );

        UserAuthenticationInfo original = new UserAuthenticationInfo(user, authOption);
        original.setPasswordExpired(false);
        original.setLock(false);

        UserAuthenticationInfo cloned = new UserAuthenticationInfo(original);

        Assertions.assertEquals(original.getAuthPlugin(), cloned.getAuthPlugin());
        Assertions.assertEquals(original.getAuthString(), cloned.getAuthString());
        Assertions.assertArrayEquals(original.getPassword(), cloned.getPassword());
        Assertions.assertEquals(original.isPasswordExpired(), cloned.isPasswordExpired());
        Assertions.assertEquals(original.isLock(), cloned.isLock());
    }

    @Test
    public void testCloneWithAnyUserAndAnyHost() {
        // Test clone with wildcard user and host
        UserRef user = new UserRef("%", "%");
        UserAuthOption authOption = new UserAuthOption(
                AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString(),
                "password",
                true,
                NodePosition.ZERO
        );

        UserAuthenticationInfo original = new UserAuthenticationInfo(user, authOption);
        UserAuthenticationInfo cloned = new UserAuthenticationInfo(original);

        // Verify pattern matching works for wildcards
        Assertions.assertTrue(original.matchUser("anyuser"));
        Assertions.assertTrue(cloned.matchUser("anyuser"));
        Assertions.assertTrue(original.matchHost("anyhost"));
        Assertions.assertTrue(cloned.matchHost("anyhost"));

        Assertions.assertEquals(original.matchUser("testuser"), cloned.matchUser("testuser"));
        Assertions.assertEquals(original.matchHost("localhost"), cloned.matchHost("localhost"));
    }
}

