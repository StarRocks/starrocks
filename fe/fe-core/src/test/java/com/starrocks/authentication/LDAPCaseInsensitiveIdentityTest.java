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
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AuthenticationAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.directory.BasicAttributes;

/**
 * What the two LDAP case-insensitivity switches change once a login has been authenticated: which
 * stored user it resolves to, what name the session ends up carrying, and how group names compare.
 * The directory itself is mocked away; these tests are about the StarRocks side.
 */
public class LDAPCaseInsensitiveIdentityTest {
    private static final byte[] AUTH_RESPONSE = "password\0".getBytes(StandardCharsets.UTF_8);
    private static final String LDAP_SI = "ldap_si";
    private static final String LDAP_GROUP_PROVIDER = "ldap_gp";
    private static final String SEARCH_ATTR = "uid";

    /** The spelling the mocked directory holds, deliberately different from anything a test types. */
    private static final String DIRECTORY_SPELLING = "Allen";

    private boolean savedIdentitySwitch;
    private String[] savedAuthChain;
    private String[] savedGroupProvider;

    private AuthenticationMgr authenticationMgr;

    @BeforeEach
    public void setUp() throws Exception {
        savedIdentitySwitch = Config.authentication_ldap_case_insensitive;
        savedAuthChain = Config.authentication_chain;
        savedGroupProvider = Config.group_provider;

        // Set both switches explicitly rather than inheriting whatever the defaults happen to be, so
        // that changing a default later does not silently change what these tests assert.
        Config.authentication_ldap_case_insensitive = false;

        UtFrameUtils.setUpForPersistTest();

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // no scheduled refresh, each test sets the cache directly
            }
        };
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected void checkPassword(String dn, String password) throws Exception {
                // the directory accepted the bind
            }

            @Mock
            protected String findUserDNByRoot(String user) throws Exception {
                return "uid=" + DIRECTORY_SPELLING + ",ou=people,dc=example,dc=com";
            }

            @Mock
            protected LdapUserEntry checkPasswordByDnPattern(String user, String password,
                                                             String[] requestedAttributes) {
                // Direct bind never searches, so it never sees the entry - hence no attributes.
                return new LdapUserEntry("uid=" + user + ",ou=people,dc=example,dc=com", null);
            }

            @Mock
            protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes) {
                // A real directory resolves the entry regardless of how the client capitalized the
                // name, and hands back the entry's own spelling of it.
                BasicAttributes attributes = new BasicAttributes(true);
                attributes.put(SEARCH_ATTR, DIRECTORY_SPELLING);
                return new LdapUserEntry("uid=" + DIRECTORY_SPELLING + ",ou=people,dc=example,dc=com", attributes);
            }
        };

        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        Config.authentication_chain = new String[] {"native"};
        Config.group_provider = new String[] {};
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_ldap_case_insensitive = savedIdentitySwitch;
        Config.authentication_chain = savedAuthChain;
        Config.group_provider = savedGroupProvider;
        UtFrameUtils.tearDownForPersisTest();
    }

    private CreateUserStmt ldapUserStmt(String name) {
        return new CreateUserStmt(
                new UserRef(name, "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                        "uid=" + name + ",ou=people,dc=example,dc=com",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO);
    }

    private void createLdapUser(String name) throws Exception {
        authenticationMgr.createUser(ldapUserStmt(name));
    }

    private void createNativeUser(String name) throws Exception {
        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef(name, "%", false, NodePosition.ZERO),
                true, null,
                List.of(), Map.of(), NodePosition.ZERO));
    }

    private void createLdapSecurityIntegration(Map<String, String> extraProperties) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name());
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN, "cn=admin,dc=example,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD, "secret");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN, "dc=example,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR, "uid");
        properties.putAll(extraProperties);
        authenticationMgr.replayCreateSecurityIntegration(LDAP_SI, properties);
        Config.authentication_chain = new String[] {"native", LDAP_SI};
    }

    private void createLdapGroupProvider(Map<String, Set<String>> cache) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");
        authenticationMgr.replayCreateGroupProvider(LDAP_GROUP_PROVIDER, properties);
        ((LDAPGroupProvider) authenticationMgr.getGroupProvider(LDAP_GROUP_PROVIDER)).setUserToGroupCache(cache);
    }

    private ConnectContext newLdapClientContext() {
        ConnectContext context = new ConnectContext();
        context.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
        return context;
    }

    // ---------------------------------------------------------------- native path

    /**
     * Test case: a user created with AUTHENTICATION_LDAP_SIMPLE logs in with a different casing.
     * Test point: the login resolves to the stored user, and the session keeps the *stored* spelling
     *             rather than the typed one, because the roles and user properties are keyed on it.
     */
    @Test
    public void testNativeLdapUserIsMatchedIgnoringCase() throws Exception {
        Config.authentication_ldap_case_insensitive = true;
        createLdapUser("ldap_user");

        for (String typed : new String[] {"ldap_user", "LDAP_USER", "Ldap_User"}) {
            ConnectContext context = newLdapClientContext();
            AuthenticationHandler.authenticate(context, typed, "%", AUTH_RESPONSE);

            Assertions.assertEquals("ldap_user", context.getQualifiedUser(),
                    "login as '" + typed + "' must resolve to the stored user");
            Assertions.assertFalse(context.getCurrentUserIdentity().isEphemeral(),
                    "the stored user must be used, otherwise its per-user DN and roles are lost");
        }
    }

    /**
     * Test case: same setup with the identity switch off (the default).
     * Test point: the relaxed lookup is gone, so the login no longer finds the stored user.
     */
    @Test
    public void testNativeLdapUserIsCaseSensitiveWhenDisabled() throws Exception {
        Config.authentication_ldap_case_insensitive = false;
        createLdapUser("ldap_user");

        AuthenticationHandler.authenticate(newLdapClientContext(), "ldap_user", "%", AUTH_RESPONSE);
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(newLdapClientContext(), "LDAP_USER", "%", AUTH_RESPONSE));
    }

    /**
     * Test case: a native-password user whose name differs only in case.
     * Test point: the relaxed lookup must never apply to native passwords, otherwise 'Root' would be
     *             a way to probe for a user deliberately created as 'root'.
     */
    @Test
    public void testNativePasswordUserStaysCaseSensitive() throws Exception {
        Config.authentication_ldap_case_insensitive = true;
        createNativeUser("native_user");

        AuthenticationHandler.authenticate(new ConnectContext(), "native_user", "%",
                "\0".getBytes(StandardCharsets.UTF_8));
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(new ConnectContext(), "Native_User", "%",
                        "\0".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Test case: the user table already holds both 'Allen' and 'allen' as LDAP users, as it may on a
     * cluster that predates the switch.
     * Test point: a login that matches both is refused rather than resolved to whichever entry the
     *             internal ordering happens to put first; an exact match still wins outright.
     */
    @Test
    public void testAmbiguousMatchIsRefused() throws Exception {
        Config.authentication_ldap_case_insensitive = false;
        createLdapUser("Allen");
        createLdapUser("allen");
        Config.authentication_ldap_case_insensitive = true;

        // Every spelling is refused, the exact ones included: under this flag the two entries denote
        // one directory account, so typing 'Allen' must not be a way to select its privilege set.
        for (String typed : new String[] {"ALLEN", "Allen", "allen"}) {
            AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                    () -> AuthenticationHandler.authenticate(newLdapClientContext(), typed, "%", AUTH_RESPONSE),
                    "login as '" + typed + "' is ambiguous and must be refused");
            Assertions.assertFalse(e.getMessage().contains("'Allen'") || e.getMessage().contains("'allen'"),
                    "the message must not disclose the stored user names, got: " + e.getMessage());
        }
    }

    /**
     * Test case: a native-password user whose name differs only in case from an LDAP user.
     * Test point: it is a separate account with its own password, so it must not be dragged into
     *             the LDAP ambiguity and refused.
     */
    @Test
    public void testNativeUserIsNotAmbiguousAgainstLdapUsers() throws Exception {
        Config.authentication_ldap_case_insensitive = false;
        createLdapUser("Allen");
        createNativeUser("allen");
        Config.authentication_ldap_case_insensitive = true;

        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "allen", "%", "\0".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("allen", context.getQualifiedUser());
    }

    /**
     * Test case: turning an existing native user into an LDAP one that collides with another.
     * Test point: ALTER has to pass the same guard as CREATE, otherwise the colliding pair can still
     *             be built in two steps.
     */
    @Test
    public void testAlterUserRejectsCaseCollision() throws Exception {
        Config.authentication_ldap_case_insensitive = true;
        createLdapUser("allen");

        UserRef altered = new UserRef("Allen", "%", false, NodePosition.ZERO);
        UserAuthOption toLdap = new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                "uid=Allen,ou=people,dc=example,dc=com", false, NodePosition.ZERO);
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> AuthenticationAnalyzer.checkNoLdapUserCaseCollision(altered, toLdap));
        Assertions.assertTrue(e.getMessage().contains("allen"), e.getMessage());
    }

    /**
     * Test case: creating an LDAP user whose name differs only in case from an existing one.
     * Test point: refused while the switch is on, so the ambiguity above cannot be created afresh.
     *             With the switch off there is nothing ambiguous about it, and it is allowed.
     */
    @Test
    public void testCreateUserRejectsCaseCollision() throws Exception {
        Config.authentication_ldap_case_insensitive = false;
        createLdapUser("allen");
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        // Switch off: no ambiguity to prevent.
        AuthenticationAnalyzer.checkNoLdapUserCaseCollision(
                ldapUserStmt("Allen").getUser(), ldapUserStmt("Allen").getAuthOption());

        Config.authentication_ldap_case_insensitive = true;
        CreateUserStmt colliding = ldapUserStmt("Allen");
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> AuthenticationAnalyzer.checkNoLdapUserCaseCollision(
                        colliding.getUser(), colliding.getAuthOption()));
        Assertions.assertTrue(e.getMessage().contains("allen"), e.getMessage());

        // The same name is not a collision with itself.
        CreateUserStmt sameName = ldapUserStmt("allen");
        AuthenticationAnalyzer.checkNoLdapUserCaseCollision(sameName.getUser(), sameName.getAuthOption());
    }

    /**
     * Test case: the same collision seen through the plain lookup rather than the login one.
     * Test point: it reports no match instead of picking a candidate. That accessor serves the
     *             MySQL handshake's plugin negotiation, stream load and the Kerberos provider, none
     *             of which can throw an authentication error, so "none" is the only safe answer.
     */
    @Test
    public void testPlainLookupReportsNoMatchWhenAmbiguous() throws Exception {
        Config.authentication_ldap_case_insensitive = false;
        createLdapUser("Allen");
        createLdapUser("allen");
        Config.authentication_ldap_case_insensitive = true;

        // Every spelling, the exact ones included: the collision scan runs even on an exact hit, so
        // this accessor and the login agree that no single user can be named. Reporting a candidate
        // here while the login refuses it would just move the surprise elsewhere.
        for (String typed : new String[] {"ALLEN", "Allen", "allen"}) {
            Assertions.assertNull(authenticationMgr.getBestMatchedUserIdentity(typed, "%"),
                    "'" + typed + "' names two LDAP users, so there is no single match to report");
        }
    }

    // ---------------------------------------------------------------- security integration path

    /**
     * Test case: a user with no entry in the user table logs in through an LDAP security integration.
     * Test point: the session identity is the spelling the directory holds, not a lowercase form
     *             StarRocks invented and not the casing the client happened to type.
     */
    @Test
    public void testSecurityIntegrationTakesNameFromDirectory() throws Exception {
        Config.authentication_ldap_case_insensitive = true;
        createLdapSecurityIntegration(Map.of());

        for (String typed : new String[] {"Allen", "aLLEN", "ALLEN", "allen"}) {
            ConnectContext context = newLdapClientContext();
            AuthenticationHandler.authenticate(context, typed, "%", AUTH_RESPONSE);

            Assertions.assertEquals(DIRECTORY_SPELLING, context.getQualifiedUser(),
                    "login as '" + typed + "' must collapse to the directory's spelling");
            Assertions.assertEquals(DIRECTORY_SPELLING, context.getCurrentUserIdentity().getUser());
        }
    }

    /**
     * Test case: the security integration binds directly through a DN pattern, so it never searches
     * and never sees the entry.
     * Test point: with no authoritative spelling to take, the identity falls back to lowercase.
     */
    @Test
    public void testSecurityIntegrationFallsBackToLowercaseOnDirectBind() throws Exception {
        Config.authentication_ldap_case_insensitive = true;
        createLdapSecurityIntegration(Map.of(
                SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=people,dc=example,dc=com"));

        ConnectContext context = newLdapClientContext();
        AuthenticationHandler.authenticate(context, "aLLEN", "%", AUTH_RESPONSE);
        Assertions.assertEquals("allen", context.getQualifiedUser());
    }

    /**
     * Test case: the search found the entry, but it came back without the attribute the filter used.
     * Test point: the identity falls back to lowercase rather than to null or to the typed casing.
     *             A directory can be configured to withhold the attribute, and that must not leave
     *             the session with no name.
     */
    @Test
    public void testSecurityIntegrationFallsBackWhenTheEntryHasNoName() throws Exception {
        Config.authentication_ldap_case_insensitive = true;
        createLdapSecurityIntegration(Map.of());

        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected void checkPassword(String dn, String password) throws Exception {
            }

            @Mock
            protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes) {
                return new LdapUserEntry("uid=" + DIRECTORY_SPELLING + ",ou=people,dc=example,dc=com",
                        new BasicAttributes(true));
            }
        };

        ConnectContext context = newLdapClientContext();
        AuthenticationHandler.authenticate(context, "aLLEN", "%", AUTH_RESPONSE);
        Assertions.assertEquals("allen", context.getQualifiedUser());
    }

    /**
     * Test case: same setup with the identity switch off.
     * Test point: the identity keeps the typed casing. Authentication itself still succeeds, because
     *             the directory matches the name regardless of case on its own.
     */
    @Test
    public void testSecurityIntegrationKeepsTypedNameWhenDisabled() throws Exception {
        Config.authentication_ldap_case_insensitive = false;
        createLdapSecurityIntegration(Map.of());

        ConnectContext context = newLdapClientContext();
        AuthenticationHandler.authenticate(context, "aLLEN", "%", AUTH_RESPONSE);
        Assertions.assertEquals("aLLEN", context.getQualifiedUser());
    }

    // ---------------------------------------------------------------- group side

    /**
     * Test case: permitted_groups written with stray whitespace around the items.
     * Test point: "  finance , ENGINEERING ,ops " still names three groups, not one padded string.
     */
    @Test
    public void testPermittedGroupsAreTrimmed() throws Exception {
        createLdapGroupProvider(new HashMap<>(Map.of("allen", Set.of("Engineering", "Ops"))));
        createLdapSecurityIntegration(Map.of(
                SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, LDAP_GROUP_PROVIDER,
                SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "  finance , ENGINEERING ,ops "));

        AuthenticationHandler.authenticate(newLdapClientContext(), "Allen", "%", AUTH_RESPONSE);
    }

    /**
     * Test case: the user really is in none of the permitted groups.
     * Test point: the login is refused, and the message names both sides so an administrator can see
     *             which one is wrong without turning on debug logging.
     */
    @Test
    public void testPermittedGroupsDenyMessageNamesBothSides() throws Exception {
        createLdapGroupProvider(new HashMap<>(Map.of("allen", Set.of("Engineering"))));
        createLdapSecurityIntegration(Map.of(
                SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, LDAP_GROUP_PROVIDER,
                SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "finance"));

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(newLdapClientContext(), "Allen", "%", AUTH_RESPONSE));
        Assertions.assertTrue(e.getMessage().contains("Engineering"),
                "the message must show the groups the directory returned, got: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("permitted_groups[finance]"),
                "the message must show what permitted_groups was set to, got: " + e.getMessage());
    }

}
