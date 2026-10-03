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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which LDAP group provider configurations the Thrift-rebuilt authorization context can resolve groups for.
 *
 * <p>{@code UserIdentityUtils.setGroupsIfAbsent} re-resolves the groups that TAuthInfo/TUserIdentity do not
 * carry, and it has no distinguished name to look them up by: the DN is produced by the LDAP bind and only
 * the authenticating session holds it. So it passes the user name, and whether that resolves is decided
 * entirely by {@code ldap_user_search_attr}. Both halves are pinned here because the two configurations
 * behave oppositely and only one of them is the deployed one.
 */
public class UserIdentityUtilsGroupLookupKeyTest {
    /** The name a user logs in with, and the CN inside that user's member DN. */
    private static final String USER = "someone";
    private static final String USER_DN = "cn=someone,ou=ldapusers,ou=woowahan,dc=woowahan,dc=in";
    private static final Set<String> GROUPS = Set.of("zeppelin-public", "dataservice");

    /** The deployed value of {@code ldap_user_search_attr}: a regex whose capture group is the user name. */
    private static final String USER_SEARCH_ATTR = "CN=([^,]+)";

    private String[] savedGroupProvider;
    /**
     * Surefire runs a module's test classes in one JVM (the default {@code forkCount=1},
     * {@code reuseForks=true}), so the manager {@link #registerLdapGroupProvider} swaps in would otherwise
     * outlive this class and serve whatever runs next - a manager holding one throwaway group provider and
     * no users.
     */
    private AuthenticationMgr savedAuthenticationMgr;

    @BeforeEach
    public void setUp() {
        savedGroupProvider = Config.group_provider;
        savedAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // No directory to reach and no refresh thread to start - the tests set the cache directly.
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.group_provider = savedGroupProvider;
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(savedAuthenticationMgr);
    }

    /**
     * Registers a single ldap group provider and makes it the global one, so
     * {@code setGroupsIfAbsent}'s {@code List.of(Config.group_provider)} finds exactly it.
     *
     * <p>Swaps in a manager of its own rather than adding to the shared one, so the providers registered
     * here leave nothing behind. {@link #tearDown} puts the original back.
     */
    private static LDAPGroupProvider registerLdapGroupProvider(String name, Map<String, String> extraProperties)
            throws DdlException {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>(extraProperties);
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        authenticationMgr.replayCreateGroupProvider(name, properties);

        Config.group_provider = new String[] {name};
        return (LDAPGroupProvider) authenticationMgr.getGroupProvider(name);
    }

    private static TUserIdentity thriftIdentity(String user) {
        TUserIdentity tUserIdent = new TUserIdentity();
        tUserIdent.setUsername(user);
        tUserIdent.setHost("%");
        tUserIdent.setIs_domain(false);
        return tUserIdent;
    }

    /**
     * The deployed configuration. With {@code ldap_user_search_attr} set, a refresh pulls the user name out
     * of each member DN and caches under it, so the user name is the correct - and only needed - key.
     */
    @Test
    public void testUserNameKeyedProviderResolvesGroups() throws DdlException {
        LDAPGroupProvider provider = registerLdapGroupProvider("user_name_keyed_provider",
                Map.of(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, USER_SEARCH_ATTR));
        provider.setUserToGroupCache(Map.of(USER, GROUPS));

        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, thriftIdentity(USER));

        Assertions.assertEquals(GROUPS, context.getGroups());
    }

    /**
     * The gap the review asked about. Without {@code ldap_user_search_attr} the cache is keyed by the full
     * member DN, which no Thrift request carries, so this path resolves nothing.
     *
     * <p>Asserted rather than fixed on purpose: empty groups deny rather than grant, and closing it means
     * carrying the DN or the groups themselves over Thrift plus a BE built against the widened schema.
     * If a deployment ever drops {@code ldap_user_search_attr}, this test is what says what it costs.
     */
    @Test
    public void testDnKeyedProviderResolvesNoGroupsFromThrift() throws DdlException {
        LDAPGroupProvider provider = registerLdapGroupProvider("dn_keyed_provider", Map.of());
        provider.setUserToGroupCache(Map.of(USER_DN, GROUPS));

        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, thriftIdentity(USER));

        Assertions.assertTrue(context.getGroups().isEmpty());
        // The identity still has to be usable: the metadata scan runs, it just sees less.
        Assertions.assertEquals(USER, context.getCurrentUserIdentity().getUser());
        Assertions.assertNotNull(context.getCurrentRoleIds());
    }

    /**
     * The other side of the test above: the very same DN-keyed provider does resolve for the caller that
     * holds the DN. That is the login path, and it is why the configuration works at all despite the above.
     */
    @Test
    public void testDnKeyedProviderResolvesForTheAuthenticatedSession() throws DdlException {
        LDAPGroupProvider provider = registerLdapGroupProvider("dn_keyed_provider_at_login", Map.of());
        provider.setUserToGroupCache(Map.of(USER_DN, GROUPS));

        Set<String> groups = AuthenticationHandler.getGroups(
                UserIdentity.createEphemeralUserIdent(USER, "%"), USER_DN,
                List.of("dn_keyed_provider_at_login"));

        Assertions.assertEquals(GROUPS, groups);
    }

    /**
     * A DN-keyed provider handed no DN must answer empty. The cache is frozen with {@code Map.copyOf}, and
     * an immutable map throws on a null key rather than reporting a miss - which would surface as a
     * resolution failure rather than as an empty group set.
     */
    @Test
    public void testDnKeyedProviderToleratesAMissingDn() throws DdlException {
        LDAPGroupProvider provider = registerLdapGroupProvider("dn_keyed_provider_null_dn", Map.of());
        provider.setUserToGroupCache(Map.of(USER_DN, GROUPS));

        Set<String> groups = provider.getGroup(UserIdentity.createEphemeralUserIdent(USER, "%"), null);

        Assertions.assertTrue(groups.isEmpty());
    }

    /** Same for a provider whose cache a failed first refresh left empty. */
    @Test
    public void testDnKeyedProviderToleratesAMissingDnOnAnEmptyCache() throws DdlException {
        LDAPGroupProvider provider = registerLdapGroupProvider("dn_keyed_provider_empty_cache", Map.of());

        Set<String> groups = provider.getGroup(UserIdentity.createEphemeralUserIdent(USER, "%"), null);

        Assertions.assertTrue(groups.isEmpty());
    }
}
