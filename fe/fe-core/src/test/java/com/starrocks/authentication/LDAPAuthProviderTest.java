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
                "uid=${USER},ou=A,dc=test,dc=com:uid=${USER},ou=B,dc=test,dc=com");

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
                "uid=${USER},ou=A,dc=test,dc=com:uid=${USER},ou=B,dc=test,dc=com");

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
                "uid=${USER},ou=A,dc=test,dc=com:uid=shared,dc=test,dc=com");

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () -> {
            provider.authenticate(authCtx, user, authResponse);
        });
        Assertions.assertTrue(ex.getMessage().contains("does not contain ${USER} placeholder"));
    }
}
