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
import javax.naming.CommunicationException;
import javax.naming.InvalidNameException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.SizeLimitExceededException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * The cache may only be replaced by a complete read of the directory. Publishing a partially collected
 * map is indistinguishable from "these users belong to no group", which denies access fleet-wide - the
 * exact failure this provider has to avoid.
 */
public class LDAPGroupProviderCachePolicyTest {

    private static final UserIdentity ALICE = UserIdentity.createEphemeralUserIdent("alice", "%");

    @Test
    public void testSuccessfulRefreshReplacesTheCache() {
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev")), 1)).when(provider).collectGroups();

        provider.refreshGroups();

        Assertions.assertEquals(Set.of("dev"), provider.getGroup(ALICE, "alice"));
    }

    @Test
    public void testFatalFailureKeepsTheLastKnownGoodCache() {
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev")), 1)).when(provider).collectGroups();
        provider.refreshGroups();

        doReturn(LDAPGroupProvider.RefreshResult.failed("bind failed")).when(provider).collectGroups();
        provider.refreshGroups();

        Assertions.assertEquals(Set.of("dev"), provider.getGroup(ALICE, "alice"),
                "a failed refresh must not blank out the groups of already-authenticated users");
    }

    @Test
    public void testIncompleteRangeRetrievalKeepsTheLastKnownGoodCache() {
        // The regression this whole change exists for: a truncated member list must never be published.
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev", "analyst")), 2)).when(provider).collectGroups();
        provider.refreshGroups();

        doReturn(LDAPGroupProvider.RefreshResult.failed(
                "range retrieval did not reach the terminal page for every group"))
                .when(provider).collectGroups();
        provider.refreshGroups();

        Assertions.assertEquals(Set.of("dev", "analyst"), provider.getGroup(ALICE, "alice"));
    }

    @Test
    public void testTruncatedPagedSearchKeepsTheLastKnownGoodCache() {
        // A referral (or any early stop) that aborts the group entry walk while pages were still pending
        // means entries were lost. Even though some groups were already collected, the partial view must
        // not replace a complete one.
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev")), 4)).when(provider).collectGroups();
        provider.refreshGroups();

        doReturn(LDAPGroupProvider.RefreshResult.failed(
                "group entry search stopped early with pages still pending, entries were lost"))
                .when(provider).collectGroups();
        provider.refreshGroups();

        Assertions.assertEquals(Set.of("dev"), provider.getGroup(ALICE, "alice"));
        Assertions.assertTrue(provider.getComment().contains("consecutiveFailures=1"), provider.getComment());
    }

    @Test
    public void testBlankingGuardRejectsASuddenlyEmptyDirectory() {
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev")), 5)).when(provider).collectGroups();
        provider.refreshGroups();

        // Zero group entries after a healthy refresh is an outage or a broken filter, not a real change.
        doReturn(collected(Map.of(), 0)).when(provider).collectGroups();
        provider.refreshGroups();

        Assertions.assertEquals(Set.of("dev"), provider.getGroup(ALICE, "alice"));
    }

    @Test
    public void testFirstRefreshMayPublishAnEmptyDirectory() {
        // With no previous snapshot to compare against there is nothing to protect, so this publishes.
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of(), 0)).when(provider).collectGroups();

        provider.refreshGroups();

        Assertions.assertEquals(Set.of(), provider.getGroup(ALICE, "alice"));
    }

    @Test
    public void testGuardDoesNotBlockLegitimateGroupMembershipRemoval() {
        // The guard counts group entries, not users. A user genuinely removed from every group must
        // still lose those groups.
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev")), 5)).when(provider).collectGroups();
        provider.refreshGroups();

        doReturn(collected(Map.of("bob", Set.of("dev")), 5)).when(provider).collectGroups();
        provider.refreshGroups();

        Assertions.assertEquals(Set.of(), provider.getGroup(ALICE, "alice"));
    }

    @Test
    public void testRefreshNeverThrows() {
        // ScheduledExecutorService cancels every future run of a task that lets an exception escape,
        // which would freeze group refresh until the FE restarts.
        LDAPGroupProvider provider = newProvider();
        doThrow(new RuntimeException("boom")).when(provider).collectGroups();

        Assertions.assertDoesNotThrow(provider::refreshGroups);
    }

    @Test
    public void testRefreshAfterUnexpectedThrowKeepsTheCache() {
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(Map.of("alice", Set.of("dev")), 1)).when(provider).collectGroups();
        provider.refreshGroups();

        doThrow(new IllegalStateException("boom")).when(provider).collectGroups();
        provider.refreshGroups();

        Assertions.assertEquals(Set.of("dev"), provider.getGroup(ALICE, "alice"));
    }

    @Test
    public void testPublishedGroupsAreImmutable() {
        LDAPGroupProvider provider = newProvider();
        doReturn(collected(new HashMap<>(Map.of("alice", Set.of("dev"))), 1)).when(provider).collectGroups();
        provider.refreshGroups();

        Set<String> groups = provider.getGroup(ALICE, "alice");
        Assertions.assertThrows(UnsupportedOperationException.class, () -> groups.add("smuggled"));
    }

    @Test
    public void testCommentReportsCacheHealth() {
        LDAPGroupProvider provider = newProvider();
        Assertions.assertTrue(provider.getComment().contains("lastSuccessAgoSec=never"), provider.getComment());

        doReturn(collected(Map.of("alice", Set.of("dev")), 3)).when(provider).collectGroups();
        provider.refreshGroups();
        String healthy = provider.getComment();
        Assertions.assertTrue(healthy.contains("cachedUsers=1"), healthy);
        Assertions.assertTrue(healthy.contains("groupEntries=3"), healthy);
        Assertions.assertTrue(healthy.contains("consecutiveFailures=0"), healthy);
        Assertions.assertFalse(healthy.contains("lastError="), healthy);

        doReturn(LDAPGroupProvider.RefreshResult.failed("connection reset")).when(provider).collectGroups();
        provider.refreshGroups();
        provider.refreshGroups();
        String failing = provider.getComment();
        Assertions.assertTrue(failing.contains("consecutiveFailures=2"), failing);
        Assertions.assertTrue(failing.contains("lastError=connection reset"), failing);
        // The cache itself is untouched, so it still reports the users it is serving.
        Assertions.assertTrue(failing.contains("cachedUsers=1"), failing);
    }

    @Test
    public void testConsecutiveFailureCountResetsOnRecovery() {
        LDAPGroupProvider provider = newProvider();
        doReturn(LDAPGroupProvider.RefreshResult.failed("nope")).when(provider).collectGroups();
        provider.refreshGroups();
        provider.refreshGroups();
        Assertions.assertTrue(provider.getComment().contains("consecutiveFailures=2"));

        doReturn(collected(Map.of("alice", Set.of("dev")), 1)).when(provider).collectGroups();
        provider.refreshGroups();
        Assertions.assertTrue(provider.getComment().contains("consecutiveFailures=0"));
    }

    // ---------- error classification ----------

    @Test
    public void testDeterministicAbsenceIsSkippable() {
        // The server answered authoritatively that the entry is not there, so the read was not truncated.
        Assertions.assertTrue(LDAPGroupProvider.isDeterministicAbsence(new NameNotFoundException("gone")));
        Assertions.assertTrue(LDAPGroupProvider.isDeterministicAbsence(new InvalidNameException("bad dn")));
    }

    @Test
    public void testFailureToAnswerIsNotSkippable() {
        // Anything that means "the server did not answer" may have truncated the read.
        Assertions.assertFalse(LDAPGroupProvider.isDeterministicAbsence(new CommunicationException("reset")));
        Assertions.assertFalse(LDAPGroupProvider.isDeterministicAbsence(new SizeLimitExceededException("cap")));
        Assertions.assertFalse(LDAPGroupProvider.isDeterministicAbsence(new PartialResultException("referral")));
        // Unclassified exceptions default to fatal rather than being silently skipped.
        Assertions.assertFalse(LDAPGroupProvider.isDeterministicAbsence(new NamingException("something new")));
    }

    // ---------- helpers ----------

    private static LDAPGroupProvider newProvider() {
        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");
        return spy(new LDAPGroupProvider("test_ldap_group_provider", properties));
    }

    private static LDAPGroupProvider.RefreshResult collected(Map<String, Set<String>> groups, int groupEntries) {
        LDAPGroupProvider.Stats stats = new LDAPGroupProvider.Stats();
        stats.groupEntries = groupEntries;
        return LDAPGroupProvider.RefreshResult.collected(groups, stats);
    }
}
