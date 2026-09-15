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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * A group provider that was already in the metadata image when the FE restarted is not constructed - it is
 * restored by GSON, which instantiates it through UnsafeAllocator. That skips the constructor AND every
 * inline field initializer, and {@code HiddenAnnotationExclusionStrategy} skips every field without
 * {@code @SerializedName}, so nothing in the JSON fills them back in either.
 *
 * <p>This is not hypothetical: it was observed on a staging cluster, where {@code SHOW GROUP PROVIDERS}
 * threw NPE against a provider baked into the image, and the scheduled refresh would have died the same
 * way - permanently, because ScheduledExecutorService cancels a task that lets an exception escape.
 *
 * <p>These tests go through the real {@link GsonUtils} instance, so they exercise the actual
 * RuntimeTypeAdapterFactory / post-process wiring rather than a hand-built Gson.
 */
public class LDAPGroupProviderImageRoundTripTest {

    private static LDAPGroupProvider roundTrip(LDAPGroupProvider provider) {
        String json = GsonUtils.GSON.toJson(provider, GroupProvider.class);
        GroupProvider restored = GsonUtils.GSON.fromJson(json, GroupProvider.class);
        Assertions.assertInstanceOf(LDAPGroupProvider.class, restored,
                "the polymorphic adapter must restore the concrete subtype");
        return (LDAPGroupProvider) restored;
    }

    private static LDAPGroupProvider newProvider() {
        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");
        return new LDAPGroupProvider("image_round_trip", properties);
    }

    @Test
    public void testPropertiesSurviveTheRoundTrip() {
        LDAPGroupProvider restored = roundTrip(newProvider());

        Assertions.assertEquals("image_round_trip", restored.getName());
        Assertions.assertEquals("ldap", restored.getType());
        Assertions.assertEquals("uid", restored.getLdapUserSearchAttr());
    }

    @Test
    public void testGetGroupDoesNotThrowOnAnImageLoadedProvider() {
        // getGroup reads userToGroupCache, which GSON leaves null without the post-process hook.
        LDAPGroupProvider restored = roundTrip(newProvider());

        Assertions.assertEquals(Set.of(),
                restored.getGroup(UserIdentity.createEphemeralUserIdent("alice", "%"),
                        "alice"));
    }

    @Test
    public void testShowGroupProvidersDoesNotThrowOnAnImageLoadedProvider() {
        // This is the exact call SHOW GROUP PROVIDERS makes, and the one that was observed to fail.
        LDAPGroupProvider restored = roundTrip(newProvider());

        String comment = Assertions.assertDoesNotThrow(restored::getComment);
        Assertions.assertTrue(comment.contains("cachedUsers=0"), comment);
        Assertions.assertTrue(comment.contains("consecutiveFailures=0"), comment);
        Assertions.assertTrue(comment.contains("lastSuccessAgoSec=never"), comment);
    }

    @Test
    public void testSuccessfulRefreshDoesNotThrowOnAnImageLoadedProvider() {
        // The success path assigns the cache and then resets the failure counter. Any reference-typed
        // runtime state here is a trap: as an AtomicInteger the counter arrived null from the image, so
        // the very first successful refresh threw AFTER publishing - killing the scheduler while looking
        // like it had worked. Hence the primitive field, and hence this test.
        LDAPGroupProvider restored = spy(roundTrip(newProvider()));
        doReturn(LDAPGroupProvider.RefreshResult.collected(
                Map.of("alice", Set.of("dev")), new LDAPGroupProvider.Stats())).when(restored).collectGroups();

        Assertions.assertDoesNotThrow(restored::refreshGroups);
        Assertions.assertEquals(Set.of("dev"),
                restored.getGroup(UserIdentity.createEphemeralUserIdent("alice", "%"),
                        "alice"));
    }

    @Test
    public void testFailedRefreshDoesNotThrowOnAnImageLoadedProvider() {
        // The failure path increments the counter and logs userToGroupCache.size(); both were NPE sites.
        LDAPGroupProvider restored = spy(roundTrip(newProvider()));
        doReturn(LDAPGroupProvider.RefreshResult.failed("bind failed")).when(restored).collectGroups();

        Assertions.assertDoesNotThrow(restored::refreshGroups);
        Assertions.assertTrue(restored.getComment().contains("consecutiveFailures=1"), restored.getComment());
    }

    @Test
    public void testRefreshNeverPropagatesWhenTheThrowIsOutsideCollectGroups() {
        // The scheduler contract: refreshGroups() must swallow everything, not just what collectGroups()
        // throws. A null RefreshResult reproduces the shape of the observed bug - collectGroups() returns
        // fine and the publish code below it blows up. If this regresses, one bad refresh silently stops
        // every future refresh for the lifetime of the FE.
        LDAPGroupProvider provider = spy(newProvider());
        doReturn(null).when(provider).collectGroups();

        Assertions.assertDoesNotThrow(provider::refreshGroups);
    }

    @Test
    public void testDestroyIsSafeOnAnImageLoadedProviderThatNeverInitialised() {
        // scheduleTask is also left null by GSON; destroy() must not NPE.
        LDAPGroupProvider restored = roundTrip(newProvider());

        Assertions.assertDoesNotThrow(restored::destroy);
    }
}
