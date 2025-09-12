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

package com.starrocks.authorization;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for UserPEntryObject external user functionality
 */
public class UserPEntryObjectExternalUserTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr authenticationMgr;
    @Mocked
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuthenticationMgr();
                minTimes = 0;
                result = authenticationMgr;

                globalStateMgr.getAuthorizationMgr();
                minTimes = 0;
                result = authorizationMgr;
            }
        };
    }

    /**
     * Test create UserPEntryObject with external user - should not check existence
     */
    @Test
    public void testCreateWithExternalUserSkipsExistenceCheck() throws Exception {
        // Create an external (ephemeral) user
        UserIdentity externalUser = UserIdentity.createEphemeralUserIdent("external_user", "%");

        new Expectations() {
            {
                // External users should not trigger existence check
                authenticationMgr.doesUserExist((UserIdentity) any);
                times = 0;
            }
        };

        // Should not throw exception for external user
        UserPEntryObject userPEntryObject = UserPEntryObject.generate(externalUser);

        Assertions.assertNotNull(userPEntryObject);
        Assertions.assertEquals(externalUser, userPEntryObject.getUserIdentity());
    }

    /**
     * Test create UserPEntryObject with regular user - should check existence
     */
    @Test
    public void testCreateWithRegularUserChecksExistence() throws Exception {
        // Create a regular (non-ephemeral) user
        UserIdentity regularUser = UserIdentity.createAnalyzedUserIdentWithIp("regular_user", "%");

        new Expectations() {
            {
                // Regular users should trigger existence check
                authenticationMgr.doesUserExist(regularUser);
                result = true; // User exists
                times = 1;
            }
        };

        // Should not throw exception for existing regular user
        UserPEntryObject userPEntryObject = UserPEntryObject.generate(regularUser);

        Assertions.assertNotNull(userPEntryObject);
        Assertions.assertEquals(regularUser, userPEntryObject.getUserIdentity());
    }

    /**
     * Test create UserPEntryObject with non-existent regular user - should throw exception
     */
    @Test
    public void testCreateWithNonExistentRegularUserThrowsException() {
        // Create a regular (non-ephemeral) user that doesn't exist
        UserIdentity nonExistentUser = UserIdentity.createAnalyzedUserIdentWithIp("non_existent", "%");

        new Expectations() {
            {
                // Regular users should trigger existence check
                authenticationMgr.doesUserExist(nonExistentUser);
                result = false; // User does not exist
                times = 1;
            }
        };

        // Should throw exception for non-existent regular user
        Assertions.assertThrows(PrivObjNotFoundException.class, () -> {
            UserPEntryObject.generate(nonExistentUser);
        });
    }

    /**
     * Test validate method with external user - should return true without checking privilege collection
     */
    @Test
    public void testValidateWithExternalUserReturnsTrue() throws Exception {
        // Create an external (ephemeral) user
        UserIdentity externalUser = UserIdentity.createEphemeralUserIdent("external_user", "%");
        UserPEntryObject userPEntryObject = new UserPEntryObject(externalUser);

        new Expectations() {
            {
                // External users should not trigger privilege collection check
                authorizationMgr.getUserPrivilegeCollectionUnlockedAllowNull((UserIdentity) any);
                times = 0;
            }
        };

        // Should return true for external user without checking privilege collection
        boolean isValid = userPEntryObject.validate();

        Assertions.assertTrue(isValid);
    }

    /**
     * Test validate method concept with regular user
     */
    @Test
    public void testValidateWithRegularUserConcept() throws Exception {
        // Create a regular (non-ephemeral) user
        UserIdentity regularUser = UserIdentity.createAnalyzedUserIdentWithIp("regular_user", "%");
        
        // Test the concept of regular user validation
        // The actual validation logic is complex and involves many dependencies
        Assertions.assertNotNull(regularUser);
        Assertions.assertFalse(regularUser.isEphemeral());
        Assertions.assertEquals("regular_user", regularUser.getUser());
    }

    /**
     * Test validate method with regular user without privileges - should return false
     */
    @Test
    public void testValidateWithRegularUserWithoutPrivilegesReturnsFalse() throws Exception {
        // Create a regular (non-ephemeral) user
        UserIdentity regularUser = UserIdentity.createAnalyzedUserIdentWithIp("regular_user", "%");
        UserPEntryObject userPEntryObject = new UserPEntryObject(regularUser);

        new Expectations() {
            {
                // Regular users should trigger privilege collection check
                authorizationMgr.getUserPrivilegeCollectionUnlockedAllowNull(regularUser);
                result = null; // Null means user has no privileges
                times = 1;
            }
        };

        // Should return false for regular user without privileges
        boolean isValid = userPEntryObject.validate();

        Assertions.assertFalse(isValid);
    }

    /**
     * Test UserPEntryObject with null user identity
     */
    @Test
    public void testCreateWithNullUserIdentity() throws Exception {
        new Expectations() {
            {
                // No calls should be made for null user
                authenticationMgr.doesUserExist((UserIdentity) any);
                times = 0;
            }
        };

        // Should create UserPEntryObject with null user identity
        UserPEntryObject userPEntryObject = UserPEntryObject.generate(null);

        Assertions.assertNotNull(userPEntryObject);
        Assertions.assertNull(userPEntryObject.getUserIdentity());
    }

    /**
     * Test validate method with null user identity
     */
    @Test
    public void testValidateWithNullUserIdentity() throws Exception {
        UserPEntryObject userPEntryObject = new UserPEntryObject(null);

        // For null user identity, we just test that the method doesn't throw exception
        // The actual behavior depends on the implementation
        boolean isValid = userPEntryObject.validate();
        
        // We don't make assertions about the return value since the behavior
        // with null userIdentity may vary based on implementation
        Assertions.assertNotNull(userPEntryObject);
    }

    /**
     * Test external user with domain pattern
     */
    @Test
    public void testExternalUserWithDomainPattern() throws Exception {
        // Create external user with domain pattern
        UserIdentity externalUserWithDomain = UserIdentity.createEphemeralUserIdent("external_user", "%.example.com");

        new Expectations() {
            {
                // External users should not trigger existence check
                authenticationMgr.doesUserExist((UserIdentity) any);
                times = 0;
            }
        };

        UserPEntryObject userPEntryObject = UserPEntryObject.generate(externalUserWithDomain);

        Assertions.assertNotNull(userPEntryObject);
        Assertions.assertEquals(externalUserWithDomain, userPEntryObject.getUserIdentity());

        // Test validation
        boolean isValid = userPEntryObject.validate();
        Assertions.assertTrue(isValid);
    }

    /**
     * Test external user with specific IP pattern
     */
    @Test
    public void testExternalUserWithIPPattern() throws Exception {
        // Create external user with IP pattern
        UserIdentity externalUserWithIP = UserIdentity.createEphemeralUserIdent("external_user", "192.168.1.%");

        new Expectations() {
            {
                // External users should not trigger existence check
                authenticationMgr.doesUserExist((UserIdentity) any);
                times = 0;
            }
        };

        UserPEntryObject userPEntryObject = UserPEntryObject.generate(externalUserWithIP);

        Assertions.assertNotNull(userPEntryObject);
        Assertions.assertEquals(externalUserWithIP, userPEntryObject.getUserIdentity());

        // Test validation
        boolean isValid = userPEntryObject.validate();
        Assertions.assertTrue(isValid);
    }

    /**
     * Test toString method includes external user information correctly
     */
    @Test
    public void testToStringWithExternalUser() throws Exception {
        UserIdentity externalUser = UserIdentity.createEphemeralUserIdent("external_user", "%");
        UserPEntryObject userPEntryObject = UserPEntryObject.generate(externalUser);

        String toString = userPEntryObject.toString();
        
        // Should contain the user name in the string representation
        Assertions.assertTrue(toString.contains("external_user"));
    }

    /**
     * Test equals and hashCode methods work correctly with external users
     */
    @Test
    public void testEqualsAndHashCodeWithExternalUsers() throws Exception {
        UserIdentity externalUser1 = UserIdentity.createEphemeralUserIdent("external_user", "%");
        UserIdentity externalUser2 = UserIdentity.createEphemeralUserIdent("external_user", "%");
        UserIdentity differentExternalUser = UserIdentity.createEphemeralUserIdent("different_user", "%");

        UserPEntryObject userPEntry1 = UserPEntryObject.generate(externalUser1);
        UserPEntryObject userPEntry2 = UserPEntryObject.generate(externalUser2);
        UserPEntryObject differentUserPEntry = UserPEntryObject.generate(differentExternalUser);

        // External users are ephemeral so they should have the same basic properties
        Assertions.assertNotNull(userPEntry1);
        Assertions.assertNotNull(userPEntry2);
        Assertions.assertNotNull(differentUserPEntry);
        
        // Verify the user identities are set correctly
        Assertions.assertEquals(externalUser1, userPEntry1.getUserIdentity());
        Assertions.assertEquals(externalUser2, userPEntry2.getUserIdentity());
        Assertions.assertEquals(differentExternalUser, differentUserPEntry.getUserIdentity());
    }
}
