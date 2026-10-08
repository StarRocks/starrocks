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
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

/**
 * The whole login chain, from the LDAP provider through {@link AuthenticationHandler} to the group
 * set on the session: the union of the two sources, the `permitted_groups` gate and the group source
 * of a pre-created user.
 * <p>
 * Only the two methods that talk to the directory are stubbed, so the group source dispatch, the
 * merge and the gate all run for real. What the directory itself does is covered one layer down, in
 * LDAPMemberOfDirectoryTest.
 */
public class LDAPMemberOfGroupResolutionTest {
    private static final String SI_NAME = "ldap_si";
    private static final String GP_NAME = "ldap_gp";
    private static final String USER = "testuser";
    private static final String USER_DN = "uid=" + USER + ",ou=People,dc=starrocks,dc=com";

    /**
     * Only reachable through the `memberOf` attribute of the user's own entry - the group provider
     * below does not know it. The directory publishes this exact spelling and nothing may rewrite it.
     */
    private static final String MEMBEROF_ONLY_GROUP = "SR Analysts";
    /** Only reachable through the group provider, so the two sources can be told apart. */
    private static final String PROVIDER_ONLY_GROUP = "Data Platform";

    private static final byte[] PASSWORD = "password\0".getBytes(StandardCharsets.UTF_8);

    private String[] savedAuthChain;
    private String[] savedGroupProvider;
    private String savedGroupSource;

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() {
        savedAuthChain = Config.authentication_chain;
        savedGroupProvider = Config.group_provider;
        savedGroupSource = Config.authentication_ldap_simple_group_source;

        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());
        // The group provider must not try to reach a directory; the cache is injected instead.
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedAuthChain;
        Config.group_provider = savedGroupProvider;
        Config.authentication_ldap_simple_group_source = savedGroupSource;
    }

    /**
     * Replace only what talks to the directory: the search that resolves the DN plus the attributes,
     * and the user bind. Everything above stays real.
     */
    private static void stubDirectory() {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes) {
                BasicAttributes attributes = new BasicAttributes(true);
                attributes.put(new BasicAttribute("memberOf",
                        "CN=" + MEMBEROF_ONLY_GROUP + ",OU=Groups,DC=starrocks,DC=com"));
                return new LdapUserEntry(USER_DN, attributes);
            }

            @Mock
            protected String findUserDNByRoot(String user) {
                return USER_DN;
            }

            @Mock
            protected void checkPassword(String dn, String password) {
            }
        };
    }

    private static AuthenticationMgr authMgr() {
        return GlobalStateMgr.getCurrentState().getAuthenticationMgr();
    }

    /** A security integration whose LDAP settings are all stubbed out anyway. */
    private static void createSecurityIntegration(String groupSource, String groupProvider, String permittedGroups)
            throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN, "cn=svc,dc=starrocks,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD, "secret");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN, "dc=starrocks,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR, "uid");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE, groupSource);
        if (groupProvider != null) {
            properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, groupProvider);
        }
        if (permittedGroups != null) {
            properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, permittedGroups);
        }
        authMgr().replayCreateSecurityIntegration(SI_NAME, properties);
        Config.authentication_chain = new String[] {"native", SI_NAME};
    }

    /** A group provider that resolves the login name to {@link #PROVIDER_ONLY_GROUP} and nothing else. */
    private static void createGroupProvider() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");
        authMgr().replayCreateGroupProvider(GP_NAME, properties);
        LDAPGroupProvider provider = (LDAPGroupProvider) authMgr().getGroupProvider(GP_NAME);
        Map<String, Set<String>> cache = new HashMap<>();
        cache.put(USER, Set.of(PROVIDER_ONLY_GROUP));
        provider.setUserToGroupCache(cache);
    }

    private static ConnectContext login(String user) throws AuthenticationException {
        ConnectContext context = new ConnectContext();
        // The auth chain only offers a security integration to a client that negotiated the matching
        // plugin; for authentication_ldap_simple that is mysql_clear_password.
        context.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
        AuthenticationHandler.authenticate(context, user, "127.0.0.1", PASSWORD);
        return context;
    }

    /**
     * AC-5 plus the forward half of AC-9: `permitted_groups` is written in lower case while the
     * directory publishes the group in mixed case. Before the relaxation this login was refused.
     */
    @Test
    public void testPermittedGroupsAdmitsAMemberOfGroupSpelledInAnotherCase() throws Exception {
        stubDirectory();
        createSecurityIntegration("memberof", null, "sr analysts");

        ConnectContext context = login(USER);

        Assertions.assertEquals(Set.of(MEMBEROF_ONLY_GROUP), context.getGroups());
        // The reverse half: the string handed on is the directory's, not the one from the property.
        Assertions.assertTrue(context.getGroups().contains(MEMBEROF_ONLY_GROUP));
        Assertions.assertFalse(context.getGroups().contains("sr analysts"));
    }

    /**
     * The gate still rejects when nothing matches - the relaxation must not turn into "allow all".
     */
    @Test
    public void testPermittedGroupsStillRejectsWhenNoGroupMatches() throws Exception {
        stubDirectory();
        createSecurityIntegration("memberof", null, "some other group");

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () -> login(USER));
        Assertions.assertTrue(e.getMessage().contains(USER), e.getMessage());
    }

    /**
     * `memberof` means "ignore the group providers": a configured provider that would resolve a group
     * must contribute nothing, and must not even be called.
     */
    @Test
    public void testMemberOfModeIgnoresTheGroupProviders() throws Exception {
        stubDirectory();
        createGroupProvider();
        createSecurityIntegration("memberof", GP_NAME, null);

        // `memberof` must short-circuit, not call-and-discard: the reason it matters is fault
        // isolation - a broken group provider must not be able to affect a mode that excludes it.
        // Asserting only on the resulting set would still pass if the provider were called first.
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                throw new IllegalStateException(
                        "the group provider must not be consulted when group_source = memberof");
            }
        };

        ConnectContext context = login(USER);

        Assertions.assertEquals(Set.of(MEMBEROF_ONLY_GROUP), context.getGroups());
        // The provider is only ignored, not dropped - switching back is a single ALTER.
        Assertions.assertNotNull(authMgr().getGroupProvider(GP_NAME));
        Assertions.assertEquals(List.of(GP_NAME), authMgr().getSecurityIntegration(SI_NAME).getGroupProviderName());
    }

    /**
     * AC-4: `both` is the union, and it is a strict superset of either source on its own.
     */
    @Test
    public void testBothModeUnionsTheTwoSources() throws Exception {
        stubDirectory();
        createGroupProvider();
        createSecurityIntegration("both", GP_NAME, null);

        ConnectContext context = login(USER);

        Assertions.assertEquals(Set.of(MEMBEROF_ONLY_GROUP, PROVIDER_ONLY_GROUP), context.getGroups());
    }

    /**
     * The default value keeps the old behavior: only the group provider contributes, and the
     * directory is not asked for any attribute.
     */
    @Test
    public void testDefaultGroupSourceOnlyUsesTheGroupProvider() throws Exception {
        stubDirectory();
        createGroupProvider();
        createSecurityIntegration("group_provider", GP_NAME, null);

        ConnectContext context = login(USER);

        Assertions.assertEquals(Set.of(PROVIDER_ONLY_GROUP), context.getGroups());
        Assertions.assertTrue(context.getAccessControlContext().getMemberOfGroups().isEmpty());
    }

    /**
     * AC-13, the positive half: a pre-created user declared **without** `AS '<dn>'` does not go
     * through a security integration at all - its provider is built from the FE configuration - and
     * it must still pick up the memberOf groups.
     */
    @Test
    public void testPreCreatedUserWithoutAsGetsMemberOfGroupsFromTheFeConfig() throws Exception {
        stubDirectory();
        createGroupProvider();
        Config.group_provider = new String[] {GP_NAME};
        Config.authentication_ldap_simple_group_source = "both";

        authMgr().createUser(new CreateUserStmt(
                new UserRef(USER, "%", false, NodePosition.ZERO),
                true,
                // no AS '<dn>': the DN is resolved at login time
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE", null, false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext context = login(USER);

        Assertions.assertEquals(Set.of(MEMBEROF_ONLY_GROUP, PROVIDER_ONLY_GROUP), context.getGroups());
        Assertions.assertEquals(USER_DN, context.getDistinguishedName());
    }

    /**
     * AC-13, the negative half at chain level: the legacy `AS '<dn>'` form is excluded even when the
     * FE configuration turns memberOf on.
     */
    @Test
    public void testPreCreatedUserWithAsIsUnaffectedByTheFeConfig() throws Exception {
        stubDirectory();
        createGroupProvider();
        Config.group_provider = new String[] {GP_NAME};
        Config.authentication_ldap_simple_group_source = "both";

        authMgr().createUser(new CreateUserStmt(
                new UserRef(USER, "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE", USER_DN, false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext context = login(USER);

        Assertions.assertEquals(Set.of(PROVIDER_ONLY_GROUP), context.getGroups());
        Assertions.assertTrue(context.getAccessControlContext().getMemberOfGroups().isEmpty());
    }
}
