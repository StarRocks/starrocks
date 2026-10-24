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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Covers an LDAP group provider that the FE restored from its image rather than built with CREATE.
 *
 * <p>The image path goes through Gson, which used to allocate the provider without running any
 * constructor, so its group cache started out null. Every login before the first successful refresh then
 * failed with a NullPointerException, and a refresh that failed hit the same null, threw out of the
 * scheduled task and stopped all later refreshes.
 *
 * <p>Every provider here points at a closed port, so any refresh it runs fails.
 */
class LDAPGroupProviderImageLoadTest {

    private static final UserIdentity ALICE = UserIdentity.createEphemeralUserIdent("alice", "%");
    private static final String ALICE_DN = "uid=alice,ou=People,dc=starrocks,dc=com";

    /** Port 1 is privileged and closed, so the bind fails immediately rather than hanging. */
    static LDAPGroupProvider unreachableProvider() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ldap");
        properties.put("ldap_conn_url", "ldap://127.0.0.1:1");
        properties.put("ldap_bind_root_dn", "cn=admin,dc=starrocks,dc=com");
        properties.put("ldap_bind_root_pwd", "pwd");
        properties.put("ldap_bind_base_dn", "dc=starrocks,dc=com");
        properties.put("ldap_group_filter", "(objectClass=groupOfNames)");
        properties.put("ldap_user_search_attr", "uid");
        properties.put("ldap_conn_timeout", "1000");
        properties.put("ldap_conn_read_timeout", "1000");
        return new LDAPGroupProvider("test_provider", properties);
    }

    /** Serializes and restores the provider the way the image does: as a polymorphic GroupProvider. */
    static LDAPGroupProvider restoreAsFromImage(LDAPGroupProvider provider) {
        String json = GsonUtils.GSON.toJson(provider, GroupProvider.class);
        GroupProvider restored = GsonUtils.GSON.fromJson(json, GroupProvider.class);
        Assertions.assertInstanceOf(LDAPGroupProvider.class, restored);
        return (LDAPGroupProvider) restored;
    }

    /**
     * Test case: a provider restored from the image, before any refresh has run
     * Test point: the lookup answers an empty group set instead of throwing, and the persisted
     *             properties survive the round trip.
     */
    @Test
    public void testRestoredProviderAnswersBeforeFirstRefresh() {
        LDAPGroupProvider restored = restoreAsFromImage(unreachableProvider());

        Assertions.assertEquals("test_provider", restored.getName());
        Assertions.assertEquals("(objectClass=groupOfNames)", restored.getLdapGroupFilter());
        Assertions.assertEquals(Set.of(), restored.getGroup(ALICE, ALICE_DN));
    }

    /**
     * Test case: a provider restored from the image whose first refresh fails
     * Test point: the refresh neither throws (which would cancel the scheduled task for good) nor leaves the
     *             cache unusable; lookups still answer an empty group set.
     */
    @Test
    public void testRestoredProviderSurvivesFailedFirstRefresh() {
        LDAPGroupProvider restored = restoreAsFromImage(unreachableProvider());

        Assertions.assertDoesNotThrow(restored::refreshGroups);
        Assertions.assertEquals(Set.of(), restored.getGroup(ALICE, ALICE_DN));
    }

    /**
     * Test case: the real image path - AuthenticationMgr saves its group providers and a fresh manager loads
     *            them, which also starts their refresh schedules
     * Test point: the loaded provider answers lookups without throwing.
     */
    @Test
    public void testProviderLoadedByAuthenticationMgrAnswers() throws Exception {
        AuthenticationMgr saved = new AuthenticationMgr();
        saved.nameToGroupProviderMap.put("test_provider", unreachableProvider());
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        saved.saveV2(image.getImageWriter());

        AuthenticationMgr loaded = new AuthenticationMgr();
        loaded.loadV2(image.getMetaBlockReader());
        GroupProvider provider = loaded.getGroupProvider("test_provider");
        try {
            Assertions.assertInstanceOf(LDAPGroupProvider.class, provider);
            Assertions.assertEquals(Set.of(), provider.getGroup(ALICE, ALICE_DN));
        } finally {
            provider.destroy();
        }
    }

    /**
     * Test case: an unexpected runtime error in the part of the refresh that runs after the LDAP search
     * Test point: refreshGroups() does not let it escape - an exception out of a scheduleAtFixedRate task makes
     *             the executor cancel every later run, so the provider would never refresh again.
     */
    @Test
    public void testRefreshNeverThrows() {
        LDAPGroupProvider provider = new LDAPGroupProvider("test_provider", unreachableProvider().getProperties()) {
            @Override
            public long getLdapCacheMaxStaleTime() {
                throw new IllegalStateException("injected");
            }
        };

        Assertions.assertDoesNotThrow(provider::refreshGroups);
    }
}
