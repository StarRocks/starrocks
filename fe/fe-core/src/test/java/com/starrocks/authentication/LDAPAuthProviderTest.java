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
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

class LDAPAuthProviderTest {

    @BeforeAll
    public static void setUp() throws Exception {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            private void checkPassword(String dn, String password) throws Exception {
                // mock: always success
            }

            @Mock
            private String findUserDNByRoot(String user) throws Exception {
                return "uid=test,ou=People,dc=starrocks,dc=com";
            }
        };

        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef("ldap_user", "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                        "uid=ldap_user,ou=company,dc=example,dc=com",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file");
        properties.put(FileGroupProvider.GROUP_FILE_URL, "file_group");

        String groupName = "file_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
    }

    @AfterAll
    public static void teardown() throws Exception {
    }

    @Test
    void testAuthenticateSetsDNWhenLdapUserDNProvided() throws Exception {

        String providedDN = "cn=test,ou=People,dc=starrocks,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                providedDN, null);

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals(providedDN, authCtx.getDistinguishedName());
    }

    @Test
    void testAuthenticateSetsDNWhenFindByRoot() throws Exception {
        String discoveredDN = "uid=test,ou=People,dc=starrocks,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                /* ldapUserDN */ null, /* ldapBindDNPattern */ null
        );

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals(discoveredDN, authCtx.getDistinguishedName());
    }

    @Test
    void testNormalizeUsername() {
        // Test null input
        Assertions.assertNull(LDAPAuthProvider.normalizeUsername(null));

        // Test lowercase username
        Assertions.assertEquals("allen", LDAPAuthProvider.normalizeUsername("allen"));

        // Test uppercase username
        Assertions.assertEquals("allen", LDAPAuthProvider.normalizeUsername("ALLEN"));

        // Test mixed case username
        Assertions.assertEquals("allen", LDAPAuthProvider.normalizeUsername("Allen"));
        Assertions.assertEquals("allen", LDAPAuthProvider.normalizeUsername("aLLen"));
        Assertions.assertEquals("allen", LDAPAuthProvider.normalizeUsername("aLLEN"));

        // Test with numbers and special characters
        Assertions.assertEquals("user123", LDAPAuthProvider.normalizeUsername("User123"));
        Assertions.assertEquals("user_name", LDAPAuthProvider.normalizeUsername("User_Name"));

        // Test empty string
        Assertions.assertEquals("", LDAPAuthProvider.normalizeUsername(""));
    }

    @Test
    void testAuthenticateWithCaseInsensitiveUsername() throws Exception {
        // Create a test provider instance
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                /* ldapUserDN */ null, /* ldapBindDNPattern */ null
        );

        // Test that normalizeUsername works correctly for case variations
        // This verifies the internal normalization logic
        String[] testUsers = {"Allen", "aLLen", "aLLEN", "ALLEN", "allen"};
        for (String testUser : testUsers) {
            String normalized = LDAPAuthProvider.normalizeUsername(testUser);
            Assertions.assertEquals("allen", normalized,
                    "Username '" + testUser + "' should normalize to 'allen'");
        }
    }

    @Test
    void testAuthenticateByPatternSinglePattern() throws Exception {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},ou=People,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("uid=alice,ou=People,dc=test,dc=com", authCtx.getDistinguishedName());
    }

    @Test
    void testUPNPatternIsRejected() {
        // UPN-style pattern (no '=' sign) should be rejected because the result
        // is not a valid DN, which would break downstream group lookup.
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "${USER}@abc.com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () -> {
            provider.authenticate(authCtx, user, authResponse);
        });
        Assertions.assertTrue(ex.getMessage().contains("not a valid DN format"));
    }

    @Test
    void testDNPatternWithAtSignInUid() throws Exception {
        // DN pattern where uid value contains '@' is valid — '@' is a legal
        // character in DN attribute values, not a DN structural character.
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER}@abc.com,ou=People,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("uid=alice@abc.com,ou=People,dc=test,dc=com", authCtx.getDistinguishedName());
    }

    @Test
    void testPerUserDNTakesPriorityOverPattern() throws Exception {
        String perUserDN = "cn=bob,ou=Special,dc=test,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                perUserDN,
                "uid=${USER},ou=People,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("bob", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals(perUserDN, authCtx.getDistinguishedName());
    }

    @Test
    void testPatternTakesPriorityOverSearch() throws Exception {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "cn=${USER},dc=pattern,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("charlie", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("cn=charlie,dc=pattern,dc=com", authCtx.getDistinguishedName());
    }

    @Test
    void testPatternEscapesDnSpecialCharacters() throws Exception {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},dc=test,dc=com");

        // DN special characters: , + " < > ; \ should be escaped (RFC 4514)
        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice,+\"<>;\\bob", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("uid=alice\\,\\+\\\"\\<\\>\\;\\\\bob,dc=test,dc=com",
                authCtx.getDistinguishedName());
    }

    @Test
    void testPatternEscapesLeadingSpaceAndHash() throws Exception {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},dc=test,dc=com");

        // Leading space
        AccessControlContext authCtx1 = new AccessControlContext();
        UserIdentity user1 = UserIdentity.createEphemeralUserIdent(" alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);
        provider.authenticate(authCtx1, user1, authResponse);
        Assertions.assertEquals("uid=\\ alice,dc=test,dc=com", authCtx1.getDistinguishedName());

        // Leading #
        AccessControlContext authCtx2 = new AccessControlContext();
        UserIdentity user2 = UserIdentity.createEphemeralUserIdent("#alice", "%");
        provider.authenticate(authCtx2, user2, authResponse);
        Assertions.assertEquals("uid=\\#alice,dc=test,dc=com", authCtx2.getDistinguishedName());
    }

    @Test
    void testPatternFilterCharsNotEscapedInDn() throws Exception {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},dc=test,dc=com");

        // Filter special characters * ( ) | are NOT special in DN values
        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice*()|", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("uid=alice*()|,dc=test,dc=com", authCtx.getDistinguishedName());
    }

    @Test
    void testEmptyPatternFallsBackToSearch() throws Exception {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                /* ldapUserDN */ null, /* ldapBindDNPattern */ "");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("uid=test,ou=People,dc=starrocks,dc=com", authCtx.getDistinguishedName());
    }

    @Test
    void testMultiPatternFirstFailSecondSucceed() throws Exception {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected void checkPassword(String dn, String password) throws Exception {
                if (dn.contains("ou=A")) {
                    throw new AuthenticationException("bind failed for ou=A");
                }
            }
        };

        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},ou=A,dc=test,dc=com;uid=${USER},ou=B,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals("uid=alice,ou=B,dc=test,dc=com", authCtx.getDistinguishedName());

        // Restore global mock
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected void checkPassword(String dn, String password) throws Exception {
            }

            @Mock
            protected String findUserDNByRoot(String user) throws Exception {
                return "uid=test,ou=People,dc=starrocks,dc=com";
            }
        };
    }

    @Test
    void testMultiPatternAllFail() throws Exception {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected void checkPassword(String dn, String password) throws Exception {
                throw new AuthenticationException("bind failed");
            }
        };

        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},ou=A,dc=test,dc=com;uid=${USER},ou=B,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        Assertions.assertThrows(AuthenticationException.class, () -> {
            provider.authenticate(authCtx, user, authResponse);
        });

        // Restore global mock
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected void checkPassword(String dn, String password) throws Exception {
            }

            @Mock
            protected String findUserDNByRoot(String user) throws Exception {
                return "uid=test,ou=People,dc=starrocks,dc=com";
            }
        };
    }

    @Test
    void testPatternWithoutUserPlaceholderIsRejected() {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=shared_account,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () -> {
            provider.authenticate(authCtx, user, authResponse);
        });
        Assertions.assertTrue(ex.getMessage().contains("does not contain ${USER} placeholder"));
    }

    @Test
    void testMultiPatternWithOneSegmentMissingPlaceholder() {
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                null, null, null, "uid",
                /* ldapUserDN */ null,
                "uid=${USER},ou=A,dc=test,dc=com;uid=shared,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () -> {
            provider.authenticate(authCtx, user, authResponse);
        });
        Assertions.assertTrue(ex.getMessage().contains("does not contain ${USER} placeholder"));
    }

    // ==================== Group Provider integration tests ====================
    //
    // These tests verify that the distinguishedName stored after DN pattern
    // authentication is correctly used by LDAPGroupProvider for group resolution.
    //
    // Each test uses a real LDAPGroupProvider with injected userToGroups cache
    // (via setUserToGroupCache) to simulate what refreshGroups() would produce.
    // The cache key depends on ldap_user_search_attr configuration:
    //   - Not configured: key = normalized full DN (from member attribute)
    //   - Configured:     key = extracted username (by attr name or regex)

    private LDAPGroupProvider createGroupProvider(String name, String searchAttr) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "ldap");
        props.put(LDAPGroupProvider.LDAP_LDAP_CONN_URL, "ldap://localhost:389");
        props.put(LDAPGroupProvider.LDAP_PROP_ROOT_DN_KEY, "cn=admin,dc=test,dc=com");
        props.put(LDAPGroupProvider.LDAP_PROP_ROOT_PWD_KEY, "secret");
        props.put(LDAPGroupProvider.LDAP_PROP_BASE_DN_KEY, "dc=test,dc=com");
        props.put(LDAPGroupProvider.LDAP_GROUP_FILTER, "(objectClass=groupOfNames)");
        if (searchAttr != null) {
            props.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, searchAttr);
        }
        return new LDAPGroupProvider(name, props);
    }

    @SuppressWarnings("unchecked")
    private Map<String, GroupProvider> getGroupProviderMap() throws Exception {
        AuthenticationMgr mgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        java.lang.reflect.Field field = AuthenticationMgr.class.getDeclaredField("nameToGroupProviderMap");
        field.setAccessible(true);
        return (Map<String, GroupProvider>) field.get(mgr);
    }

    @Test
    void testGroupLookupByDN_SimplePattern() throws Exception {
        // Pattern: uid=${USER},ou=People,dc=test,dc=com
        // No ldap_user_search_attr → lookup by full DN
        // Cache simulates: member DN "uid=alice,ou=People,dc=test,dc=com" stored as-is
        LDAPGroupProvider gp = createGroupProvider("gp_dn_simple", null);
        Map<String, Set<String>> userToGroups = new HashMap<>();
        userToGroups.put("uid=alice,ou=people,dc=test,dc=com", Set.of("engineers"));
        gp.setUserToGroupCache(userToGroups);

        Map<String, GroupProvider> gpMap = getGroupProviderMap();
        gpMap.put("gp_dn_simple", gp);
        try {
            LDAPAuthProvider provider = new LDAPAuthProvider(
                    "localhost", 389, false, null, null,
                    null, null, null, "uid", null,
                    "uid=${USER},ou=People,dc=test,dc=com");

            AccessControlContext authCtx = new AccessControlContext();
            UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
            provider.authenticate(authCtx, user, "password\0".getBytes(StandardCharsets.UTF_8));

            Assertions.assertEquals("uid=alice,ou=People,dc=test,dc=com", authCtx.getDistinguishedName());
            Set<String> groups = AuthenticationHandler.getGroups(user, authCtx.getDistinguishedName(),
                    List.of("gp_dn_simple"));
            Assertions.assertEquals(Set.of("engineers"), groups);

            // Unknown DN → empty
            Assertions.assertTrue(AuthenticationHandler.getGroups(user,
                    "uid=unknown,ou=People,dc=test,dc=com", List.of("gp_dn_simple")).isEmpty());
        } finally {
            gpMap.remove("gp_dn_simple");
        }
    }

    @Test
    void testGroupLookupByDN_PatternWithAtSign() throws Exception {
        // Pattern: uid=${USER}@abc.com,ou=People,dc=test,dc=com
        // No ldap_user_search_attr → lookup by full DN
        // Cache simulates: member DN "uid=alice@abc.com,ou=People,dc=test,dc=com" stored as-is
        LDAPGroupProvider gp = createGroupProvider("gp_dn_at", null);
        Map<String, Set<String>> userToGroups = new HashMap<>();
        userToGroups.put("uid=alice@abc.com,ou=people,dc=test,dc=com", Set.of("staff"));
        gp.setUserToGroupCache(userToGroups);

        Map<String, GroupProvider> gpMap = getGroupProviderMap();
        gpMap.put("gp_dn_at", gp);
        try {
            LDAPAuthProvider provider = new LDAPAuthProvider(
                    "localhost", 389, false, null, null,
                    null, null, null, "uid", null,
                    "uid=${USER}@abc.com,ou=People,dc=test,dc=com");

            AccessControlContext authCtx = new AccessControlContext();
            UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
            provider.authenticate(authCtx, user, "password\0".getBytes(StandardCharsets.UTF_8));

            Assertions.assertEquals("uid=alice@abc.com,ou=People,dc=test,dc=com",
                    authCtx.getDistinguishedName());
            Set<String> groups = AuthenticationHandler.getGroups(user, authCtx.getDistinguishedName(),
                    List.of("gp_dn_at"));
            Assertions.assertEquals(Set.of("staff"), groups);
        } finally {
            gpMap.remove("gp_dn_at");
        }
    }

    @Test
    void testGroupLookupBySearchAttr_SimplePattern() throws Exception {
        // Pattern: uid=${USER},ou=People,dc=test,dc=com
        // ldap_user_search_attr=uid → refreshGroups() extracts "alice" from
        //   member DN "uid=alice,ou=People,..." → cache key = "alice"
        // getGroup() lookup key = userIdentity.getUser() = "alice" → match
        LDAPGroupProvider gp = createGroupProvider("gp_attr_simple", "uid");
        Map<String, Set<String>> userToGroups = new HashMap<>();
        userToGroups.put("alice", Set.of("developers"));
        gp.setUserToGroupCache(userToGroups);

        Map<String, GroupProvider> gpMap = getGroupProviderMap();
        gpMap.put("gp_attr_simple", gp);
        try {
            LDAPAuthProvider provider = new LDAPAuthProvider(
                    "localhost", 389, false, null, null,
                    null, null, null, "uid", null,
                    "uid=${USER},ou=People,dc=test,dc=com");

            AccessControlContext authCtx = new AccessControlContext();
            UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
            provider.authenticate(authCtx, user, "password\0".getBytes(StandardCharsets.UTF_8));

            Set<String> groups = AuthenticationHandler.getGroups(user, authCtx.getDistinguishedName(),
                    List.of("gp_attr_simple"));
            Assertions.assertEquals(Set.of("developers"), groups);
        } finally {
            gpMap.remove("gp_attr_simple");
        }
    }

    @Test
    void testGroupLookupBySearchAttr_WrongForAtSignPattern() throws Exception {
        // Pattern: uid=${USER}@abc.com,ou=People,dc=test,dc=com
        // ldap_user_search_attr=uid → refreshGroups() extracts "alice@abc.com" from
        //   member DN "uid=alice@abc.com,ou=People,..." (simple uid= extraction)
        //   → cache key = "alice@abc.com"
        // getGroup() lookup key = userIdentity.getUser() = "alice" → NO MATCH
        //
        // This demonstrates the misconfiguration: simple "uid" extraction includes
        // the @abc.com suffix, but login username is just "alice".
        LDAPGroupProvider gp = createGroupProvider("gp_attr_wrong", "uid");
        Map<String, Set<String>> userToGroups = new HashMap<>();
        userToGroups.put("alice@abc.com", Set.of("developers")); // what refreshGroups() would produce
        gp.setUserToGroupCache(userToGroups);

        Map<String, GroupProvider> gpMap = getGroupProviderMap();
        gpMap.put("gp_attr_wrong", gp);
        try {
            LDAPAuthProvider provider = new LDAPAuthProvider(
                    "localhost", 389, false, null, null,
                    null, null, null, "uid", null,
                    "uid=${USER}@abc.com,ou=People,dc=test,dc=com");

            AccessControlContext authCtx = new AccessControlContext();
            UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
            provider.authenticate(authCtx, user, "password\0".getBytes(StandardCharsets.UTF_8));

            // Lookup key "alice" vs cache key "alice@abc.com" → mismatch → empty
            Set<String> groups = AuthenticationHandler.getGroups(user, authCtx.getDistinguishedName(),
                    List.of("gp_attr_wrong"));
            Assertions.assertTrue(groups.isEmpty(),
                    "ldap_user_search_attr=uid extracts 'alice@abc.com' as cache key, " +
                    "but login username is 'alice' — use regex 'uid=([^,@]+)@abc.com' instead.");
        } finally {
            gpMap.remove("gp_attr_wrong");
        }
    }

    @Test
    void testGroupLookupBySearchAttr_CorrectRegexForAtSignPattern() throws Exception {
        // Pattern: uid=${USER}@abc.com,ou=People,dc=test,dc=com
        // ldap_user_search_attr=uid=([^,@]+)@abc.com → refreshGroups() regex extracts
        //   "alice" from member DN "uid=alice@abc.com,ou=People,..." → cache key = "alice"
        // getGroup() lookup key = userIdentity.getUser() = "alice" → match
        LDAPGroupProvider gp = createGroupProvider("gp_regex_correct", "uid=([^,@]+)@abc.com");
        Map<String, Set<String>> userToGroups = new HashMap<>();
        userToGroups.put("alice", Set.of("developers")); // regex correctly extracts "alice"
        gp.setUserToGroupCache(userToGroups);

        Map<String, GroupProvider> gpMap = getGroupProviderMap();
        gpMap.put("gp_regex_correct", gp);
        try {
            LDAPAuthProvider provider = new LDAPAuthProvider(
                    "localhost", 389, false, null, null,
                    null, null, null, "uid", null,
                    "uid=${USER}@abc.com,ou=People,dc=test,dc=com");

            AccessControlContext authCtx = new AccessControlContext();
            UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
            provider.authenticate(authCtx, user, "password\0".getBytes(StandardCharsets.UTF_8));

            Set<String> groups = AuthenticationHandler.getGroups(user, authCtx.getDistinguishedName(),
                    List.of("gp_regex_correct"));
            Assertions.assertEquals(Set.of("developers"), groups);
        } finally {
            gpMap.remove("gp_regex_correct");
        }
    }
}
