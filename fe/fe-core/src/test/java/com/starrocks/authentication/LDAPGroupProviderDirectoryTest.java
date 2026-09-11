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
import com.starrocks.common.DdlException;
import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * What {@code fetchGroupsInto()} does against a directory that answers for real, covering both ways a
 * group provider can be told where its groups are: an explicit {@code ldap_group_dn} list, read one entry
 * at a time, and an {@code ldap_group_filter} subtree search.
 * <p>
 * The other group provider tests fake this layer - {@code AlterGroupProviderLifecycleTest} mocks
 * {@code fetchGroupsInto} itself, because what it examines is the manager's activation ordering - so
 * nothing else exercises the wire format: which attributes the search asks for, how a {@code member}
 * value is turned into a cache key, or that the connection is handed back afterwards. A mock cannot show
 * any of that, since it returns the attributes the test invented.
 */
class LDAPGroupProviderDirectoryTest {
    private static final String BASE_DN = "dc=starrocks,dc=com";
    private static final String PEOPLE_DN = "ou=People," + BASE_DN;
    private static final String GROUPS_DN = "ou=Groups," + BASE_DN;
    private static final String ADMIN_DN = "cn=admin," + BASE_DN;
    private static final String ADMIN_PWD = "admin-secret";

    private static final String ANALYSTS_DN = "cn=SR Analysts," + GROUPS_DN;
    private static final String PLATFORM_DN = "cn=Data Platform," + GROUPS_DN;
    private static final String ALICE_DN = "uid=alice," + PEOPLE_DN;
    private static final String BOB_DN = "uid=bob," + PEOPLE_DN;

    private InMemoryDirectoryServer directory;
    private LDAPGroupProvider provider;

    @BeforeEach
    public void startDirectory() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(BASE_DN);
        config.addAdditionalBindCredentials(ADMIN_DN, ADMIN_PWD);
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("test", 0));

        directory = new InMemoryDirectoryServer(config);
        directory.startListening();

        directory.add("dn: " + BASE_DN, "objectClass: top", "objectClass: domain", "dc: starrocks");
        directory.add("dn: " + PEOPLE_DN, "objectClass: top", "objectClass: organizationalUnit", "ou: People");
        directory.add("dn: " + GROUPS_DN, "objectClass: top", "objectClass: organizationalUnit", "ou: Groups");
        directory.add("dn: " + ALICE_DN, "objectClass: top", "objectClass: person",
                "objectClass: organizationalPerson", "objectClass: inetOrgPerson",
                "uid: alice", "cn: alice", "sn: Anderson");
        directory.add("dn: " + BOB_DN, "objectClass: top", "objectClass: person",
                "objectClass: organizationalPerson", "objectClass: inetOrgPerson",
                "uid: bob", "cn: bob", "sn: Brown");
        // alice is in both groups, bob only in the second one.
        directory.add("dn: " + ANALYSTS_DN, "objectClass: top", "objectClass: groupOfNames",
                "cn: SR Analysts", "member: " + ALICE_DN);
        directory.add("dn: " + PLATFORM_DN, "objectClass: top", "objectClass: groupOfNames",
                "cn: Data Platform", "member: " + ALICE_DN, "member: " + BOB_DN);
    }

    @AfterEach
    public void stopDirectory() {
        if (provider != null) {
            provider.destroy();
            provider = null;
        }
        if (directory != null) {
            directory.shutDown(true);
            directory = null;
        }
    }

    /**
     * Test case: groups named one by one with `ldap_group_dn`
     * Test point: each DN is read as a single entry, the `cn` becomes the group name and every `member`
     *             value is turned into a cache key through the configured `ldap_user_search_attr`.
     */
    @Test
    public void testGroupDnListResolvesEveryMember() {
        provider = newProvider(props -> props.put(LDAPGroupProvider.LDAP_GROUP_DN,
                ANALYSTS_DN + "; " + PLATFORM_DN));

        provider.refreshGroups();

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), groupsOf("alice"),
                "alice is a member of both groups");
        Assertions.assertEquals(Set.of("Data Platform"), groupsOf("bob"),
                "bob is a member of the second group only");
        Assertions.assertEquals(Set.of(), groupsOf("carol"),
                "a user the directory does not know resolves to no group");
    }

    /**
     * Test case: groups found with `ldap_group_filter`
     * Test point: the subtree search walks the whole enumeration and yields the same mapping the explicit
     *             DN list does - this is the branch that runs against Active Directory, where group DNs are
     *             not enumerated in the configuration.
     */
    @Test
    public void testGroupFilterSearchResolvesEveryMember() {
        provider = newProvider(props -> props.put(LDAPGroupProvider.LDAP_GROUP_FILTER,
                "(objectClass=groupOfNames)"));

        provider.refreshGroups();

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), groupsOf("alice"));
        Assertions.assertEquals(Set.of("Data Platform"), groupsOf("bob"));
    }

    /**
     * Test case: no `ldap_user_search_attr`
     * Test point: without a search attribute the cache is keyed by the member DN itself, and the lookup
     *             canonicalizes the DN it is given, so a differently spelled - but equal - DN still hits.
     */
    @Test
    public void testWithoutSearchAttributeTheCacheIsKeyedByDn() {
        provider = newProvider(props -> {
            props.put(LDAPGroupProvider.LDAP_GROUP_DN, ANALYSTS_DN);
            props.remove(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR);
        });

        provider.refreshGroups();

        UserIdentity alice = UserIdentity.createEphemeralUserIdent("alice", "%");
        Assertions.assertEquals(Set.of("SR Analysts"), provider.getGroup(alice, ALICE_DN),
                "the member DN is the cache key");
        Assertions.assertEquals(Set.of("SR Analysts"),
                provider.getGroup(alice, "UID=alice, OU=People, DC=starrocks, DC=com"),
                "a DN that differs only in case and separator whitespace is the same DN");
    }

    /**
     * Test case: ALTER's synchronous activation against a live and against a wrong configuration
     * Test point: prepareForActivation() loads the cache before the provider is ever published, and reports
     *             a configuration it cannot use as a DdlException, which is what makes ALTER fail fast
     *             instead of swapping in a provider that resolves nothing.
     */
    @Test
    public void testActivationLoadsTheCacheAndRejectsBadCredentials() throws Exception {
        provider = newProvider(props -> props.put(LDAPGroupProvider.LDAP_GROUP_DN, ANALYSTS_DN));

        provider.prepareForActivation();

        Assertions.assertEquals(Set.of("SR Analysts"), groupsOf("alice"),
                "the cache is warm before init() ever starts a schedule");

        LDAPGroupProvider wrongPassword = newProvider(props ->
                props.put(LDAPGroupProvider.LDAP_PROP_ROOT_PWD_KEY, "not-the-admin-password"));
        DdlException e = Assertions.assertThrows(DdlException.class, wrongPassword::prepareForActivation,
                "a configuration the directory rejects must fail the statement");
        Assertions.assertTrue(e.getMessage().contains("failed to apply the new configuration"),
                "the error should name what went wrong: " + e.getMessage());
    }

    /**
     * Test case: `ldap_group_identifier_attr` / `ldap_group_member_attr` naming attributes the entries do
     *            not have
     * Test point: a misconfigured attribute name is the likeliest way to get an empty result out of a
     *             directory that answers perfectly well, so the group is skipped and logged rather than
     *             failing the whole fetch - which also means such a provider activates and then resolves
     *             nothing, so the skip has to stay visible in the log.
     */
    @Test
    public void testGroupsWithoutTheConfiguredAttributesAreSkipped() {
        provider = newProvider(props -> {
            props.put(LDAPGroupProvider.LDAP_GROUP_DN, ANALYSTS_DN);
            props.put(LDAPGroupProvider.LDAP_GROUP_IDENTIFIER_ATTR, "description");
        });
        provider.refreshGroups();
        Assertions.assertEquals(Set.of(), groupsOf("alice"),
                "no group name to file the members under, so nothing is cached");

        provider.destroy();
        provider = newProvider(props -> {
            props.put(LDAPGroupProvider.LDAP_GROUP_DN, ANALYSTS_DN);
            props.put(LDAPGroupProvider.LDAP_GROUP_MEMBER_ATTR, "uniqueMember");
        });
        provider.refreshGroups();
        Assertions.assertEquals(Set.of(), groupsOf("alice"),
                "the group is found but has no member attribute under that name");
    }

    private LDAPGroupProvider newProvider(java.util.function.Consumer<Map<String, String>> customize) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ldap");
        properties.put(LDAPGroupProvider.LDAP_LDAP_CONN_URL, "ldap://127.0.0.1:" + directory.getListenPort());
        properties.put(LDAPGroupProvider.LDAP_PROP_ROOT_DN_KEY, ADMIN_DN);
        properties.put(LDAPGroupProvider.LDAP_PROP_ROOT_PWD_KEY, ADMIN_PWD);
        properties.put(LDAPGroupProvider.LDAP_PROP_BASE_DN_KEY, BASE_DN);
        properties.put(LDAPGroupProvider.LDAP_SSL_CONN_ALLOW_INSECURE, "true");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");
        properties.put(LDAPGroupProvider.LDAP_GROUP_DN, ANALYSTS_DN);
        customize.accept(properties);
        return new LDAPGroupProvider("directory_test_provider", properties);
    }

    private Set<String> groupsOf(String user) {
        return provider.getGroup(UserIdentity.createEphemeralUserIdent(user, "%"), null);
    }
}
