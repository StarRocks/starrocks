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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Covers what happens to the group cache when a refresh cannot reach the directory.
 *
 * <p>Dropping the cache on the first failure used to turn a brief LDAP outage into a cluster-wide
 * login failure: with `permitted_groups` configured, an empty group set can never intersect the
 * allowed list, so every user is denied. The provider now keeps the last successful cache until it
 * has been stale for longer than `ldap_cache_max_stale_time`.
 *
 * <p>Every test here points the provider at a closed port, so `refreshGroups()` always fails; the
 * assertions are purely about which cache is left behind.
 */
class LDAPGroupProviderStaleCacheTest {

    private static final Map<String, Set<String>> CACHED = Map.of("alice", Set.of("group1"));

    /** Port 1 is privileged and closed, so the bind fails immediately rather than hanging. */
    private static LDAPGroupProvider unreachableProvider(String maxStaleTime) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ldap");
        properties.put("ldap_conn_url", "ldap://127.0.0.1:1");
        properties.put("ldap_bind_root_dn", "cn=admin,dc=starrocks,dc=com");
        properties.put("ldap_bind_root_pwd", "pwd");
        properties.put("ldap_bind_base_dn", "dc=starrocks,dc=com");
        properties.put("ldap_group_dn", "cn=group1,ou=Group,dc=starrocks,dc=com");
        properties.put("ldap_user_search_attr", "uid");
        properties.put("ldap_conn_timeout", "1000");
        properties.put("ldap_conn_read_timeout", "1000");
        if (maxStaleTime != null) {
            properties.put("ldap_cache_max_stale_time", maxStaleTime);
        }
        return new LDAPGroupProvider("test_provider", properties);
    }

    private static Set<String> groupsOf(LDAPGroupProvider provider) {
        UserIdentity alice = UserIdentity.createEphemeralUserIdent("alice", "%");
        return provider.getGroup(alice, "uid=alice,ou=People,dc=starrocks,dc=com");
    }

    @Test
    public void testKeepCacheWhenFailureIsWithinTolerance() {
        LDAPGroupProvider provider = unreachableProvider("3600");
        provider.setUserToGroupCache(new HashMap<>(CACHED));
        provider.setLastSuccessfulRefreshTimeMs(System.currentTimeMillis());

        provider.refreshGroups();

        Assertions.assertEquals(Set.of("group1"), groupsOf(provider),
                "a refresh failure inside the tolerance window must keep serving the last good cache");
    }

    @Test
    public void testDropCacheOnceStaleBeyondTolerance() {
        LDAPGroupProvider provider = unreachableProvider("60");
        provider.setUserToGroupCache(new HashMap<>(CACHED));
        // Last success was an hour ago, well past the 60s tolerance.
        provider.setLastSuccessfulRefreshTimeMs(System.currentTimeMillis() - 3600_000L);

        provider.refreshGroups();

        Assertions.assertTrue(groupsOf(provider).isEmpty(),
                "once the cache is staler than ldap_cache_max_stale_time it must be dropped");
    }

    @Test
    public void testZeroToleranceRestoresDropOnFirstFailure() {
        LDAPGroupProvider provider = unreachableProvider("0");
        provider.setUserToGroupCache(new HashMap<>(CACHED));
        provider.setLastSuccessfulRefreshTimeMs(System.currentTimeMillis() - 10L);

        provider.refreshGroups();

        Assertions.assertTrue(groupsOf(provider).isEmpty(),
                "ldap_cache_max_stale_time=0 opts back into the previous drop-immediately behaviour");
    }

    @Test
    public void testDropWhenNoRefreshEverSucceeded() {
        // lastSuccessfulRefreshTimeMs defaults to 0, so nothing has ever been fetched and there is
        // no good cache to protect, whatever the tolerance says.
        LDAPGroupProvider provider = unreachableProvider("3600");
        provider.setUserToGroupCache(new HashMap<>(CACHED));

        provider.refreshGroups();

        Assertions.assertTrue(groupsOf(provider).isEmpty(),
                "a provider that never reached the directory must not serve a cache it did not build");
    }

    @Test
    public void testDefaultToleranceIsOneHour() {
        Assertions.assertEquals(3600L, unreachableProvider(null).getLdapCacheMaxStaleTime());
    }

    @Test
    public void testMaxStaleTimeIsValidated() {
        LDAPGroupProvider provider = unreachableProvider("-1");
        Assertions.assertThrows(Exception.class, provider::checkProperty);
    }
}
