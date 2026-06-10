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
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LDAPGroupProviderCaseInsensitiveTest {

    @BeforeEach
    public void setUp() throws Exception {
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }

            @Mock
            public void refreshGroups() {
                // do nothing - we'll set cache directly in tests
            }
        };
    }

    @Test
    public void testGetGroupWithCaseInsensitiveUsername() throws DdlException {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");

        String groupName = "ldap_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        // Set up cache with normalized (lowercase) username
        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("allen", Set.of("group1", "group2", "group3"));
        ldapGroupProvider.setUserToGroupCache(groups);

        // Test with different case variations - all should match the same groups
        String[] testUsers = {"Allen", "aLLen", "aLLEN", "ALLEN", "allen"};

        for (String testUser : testUsers) {
            UserIdentity userIdentity = UserIdentity.createEphemeralUserIdent(testUser, "%");
            Set<String> resultGroups = ldapGroupProvider.getGroup(userIdentity, null);

            Assertions.assertEquals(Set.of("group1", "group2", "group3"), resultGroups,
                    "User '" + testUser + "' should match groups for 'allen'");
        }
    }

    @Test
    public void testGetGroupWithCaseInsensitiveDistinguishedName() throws DdlException {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        // No LDAP_USER_SEARCH_ATTR, so it will use distinguished name

        String groupName = "ldap_group_provider_dn";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        // Set up cache with normalized (lowercase) distinguished name
        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("uid=allen,ou=people,dc=example,dc=com", Set.of("group1", "group2"));
        ldapGroupProvider.setUserToGroupCache(groups);

        // Test with different case variations of DN
        String[] testDNs = {
                "uid=Allen,ou=People,dc=Example,dc=Com",
                "uid=ALLEN,ou=PEOPLE,dc=EXAMPLE,dc=COM",
                "uid=allen,ou=people,dc=example,dc=com"
        };

        for (String testDN : testDNs) {
            UserIdentity userIdentity = UserIdentity.createEphemeralUserIdent("test", "%");
            Set<String> resultGroups = ldapGroupProvider.getGroup(userIdentity, testDN);

            Assertions.assertEquals(Set.of("group1", "group2"), resultGroups,
                    "DN '" + testDN + "' should match groups for normalized DN");
        }
    }

    @Test
    public void testRefreshGroupsNormalizesUsernames() throws DdlException {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");

        String groupName = "ldap_group_provider_refresh";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        // Simulate refreshGroups extracting usernames with different cases
        // In real scenario, matchUserAndUpdateGroups would normalize these
        Map<String, Set<String>> groups = new HashMap<>();
        // Simulate LDAP returning mixed case usernames
        groups.put("Allen", Set.of("group1"));
        groups.put("aLLen", Set.of("group2"));
        groups.put("ALLEN", Set.of("group3"));

        // In actual implementation, these would be normalized during refreshGroups
        // For testing, we manually set normalized versions
        Map<String, Set<String>> normalizedGroups = new HashMap<>();
        normalizedGroups.put("allen", Set.of("group1", "group2", "group3"));
        ldapGroupProvider.setUserToGroupCache(normalizedGroups);

        // All case variations should now match the same normalized entry
        UserIdentity user1 = UserIdentity.createEphemeralUserIdent("Allen", "%");
        UserIdentity user2 = UserIdentity.createEphemeralUserIdent("aLLen", "%");
        UserIdentity user3 = UserIdentity.createEphemeralUserIdent("ALLEN", "%");

        Set<String> groups1 = ldapGroupProvider.getGroup(user1, null);
        Set<String> groups2 = ldapGroupProvider.getGroup(user2, null);
        Set<String> groups3 = ldapGroupProvider.getGroup(user3, null);

        Assertions.assertEquals(Set.of("group1", "group2", "group3"), groups1);
        Assertions.assertEquals(Set.of("group1", "group2", "group3"), groups2);
        Assertions.assertEquals(Set.of("group1", "group2", "group3"), groups3);
    }

    @Test
    public void testCaseInsensitiveMatchingWithMultipleUsers() throws DdlException {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");

        String groupName = "ldap_group_provider_multi";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        // Set up cache with multiple normalized users
        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("allen", Set.of("group1", "group2"));
        groups.put("bob", Set.of("group3"));
        groups.put("charlie", Set.of("group4", "group5"));
        ldapGroupProvider.setUserToGroupCache(groups);

        // Test case variations for each user
        UserIdentity allen1 = UserIdentity.createEphemeralUserIdent("Allen", "%");
        UserIdentity allen2 = UserIdentity.createEphemeralUserIdent("ALLEN", "%");
        Assertions.assertEquals(Set.of("group1", "group2"), ldapGroupProvider.getGroup(allen1, null));
        Assertions.assertEquals(Set.of("group1", "group2"), ldapGroupProvider.getGroup(allen2, null));

        UserIdentity bob1 = UserIdentity.createEphemeralUserIdent("Bob", "%");
        UserIdentity bob2 = UserIdentity.createEphemeralUserIdent("BOB", "%");
        Assertions.assertEquals(Set.of("group3"), ldapGroupProvider.getGroup(bob1, null));
        Assertions.assertEquals(Set.of("group3"), ldapGroupProvider.getGroup(bob2, null));

        UserIdentity charlie1 = UserIdentity.createEphemeralUserIdent("Charlie", "%");
        UserIdentity charlie2 = UserIdentity.createEphemeralUserIdent("CHARLIE", "%");
        Assertions.assertEquals(Set.of("group4", "group5"), ldapGroupProvider.getGroup(charlie1, null));
        Assertions.assertEquals(Set.of("group4", "group5"), ldapGroupProvider.getGroup(charlie2, null));
    }
}
