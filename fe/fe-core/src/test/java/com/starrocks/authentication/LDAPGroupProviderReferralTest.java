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

import com.starrocks.sql.ast.UserIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

/**
 * Covers a group search that ends with an exception after returning its entries, as Active Directory does.
 *
 * <p>A subtree search from an AD domain root always comes back with continuation references to the domain's
 * other partitions (DNS zones, configuration). JNDI does not follow them and throws
 * {@link PartialResultException} at the end of the enumeration, after every group has been returned. An
 * in-memory directory cannot produce this (JNDI asks it to return referral entries as plain entries), so the
 * directory here is a mocked {@link DirContext}.
 */
class LDAPGroupProviderReferralTest {

    private static final String ALICE_DN = "uid=alice,ou=People,dc=starrocks,dc=com";
    private static final String BOB_DN = "uid=bob,ou=People,dc=starrocks,dc=com";

    private static SearchResult group(String name, String... memberDns) {
        BasicAttributes attributes = new BasicAttributes(true);
        attributes.put(new BasicAttribute("cn", name));
        BasicAttribute member = new BasicAttribute("member");
        for (String dn : memberDns) {
            member.add(dn);
        }
        attributes.put(member);
        return new SearchResult("cn=" + name, null, attributes);
    }

    /**
     * Makes {@code provider} search a directory that returns two groups and then ends the enumeration with
     * {@code endOfSearch}.
     */
    @SuppressWarnings("unchecked")
    private static LDAPGroupProvider withDirectory(LDAPGroupProvider provider, NamingException endOfSearch)
            throws Exception {
        NamingEnumeration<SearchResult> results = Mockito.mock(NamingEnumeration.class);
        Mockito.when(results.hasMore()).thenReturn(true, true).thenThrow(endOfSearch);
        Mockito.when(results.next()).thenReturn(group("SR Analysts", ALICE_DN),
                group("Data Platform", ALICE_DN, BOB_DN));

        DirContext ctx = Mockito.mock(DirContext.class);
        Mockito.when(ctx.search(Mockito.anyString(), Mockito.anyString(), Mockito.any(SearchControls.class)))
                .thenReturn(results);

        LDAPGroupProvider spy = Mockito.spy(provider);
        Mockito.doReturn(ctx).when(spy).createDirContextOnConnection(Mockito.anyString(), Mockito.anyString());
        return spy;
    }

    private static Set<String> groupsOf(LDAPGroupProvider provider, String user, String dn) {
        return provider.getGroup(UserIdentity.createEphemeralUserIdent(user, "%"), dn);
    }

    private static PartialResultException referrals() {
        return new PartialResultException("Unprocessed Continuation Reference(s)");
    }

    /**
     * Test case: the search returns every group, then reports unfollowed referrals
     * Test point: the refresh counts as complete and the cache holds every group that was returned.
     */
    @Test
    public void testReferralsDoNotDiscardReturnedGroups() throws Exception {
        LDAPGroupProvider provider = withDirectory(LDAPGroupProviderImageLoadTest.unreachableProvider(), referrals());

        provider.refreshGroups();

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), groupsOf(provider, "alice", ALICE_DN));
        Assertions.assertEquals(Set.of("Data Platform"), groupsOf(provider, "bob", BOB_DN));
    }

    /**
     * Test case: a provider restored from the image whose first refresh reports referrals
     * Test point: the first refresh fills the cache, so logins resolve their groups.
     */
    @Test
    public void testRestoredProviderLoadsDespiteReferrals() throws Exception {
        LDAPGroupProvider restored = LDAPGroupProviderImageLoadTest.restoreAsFromImage(
                LDAPGroupProviderImageLoadTest.unreachableProvider());
        LDAPGroupProvider provider = withDirectory(restored, referrals());

        Assertions.assertDoesNotThrow(provider::refreshGroups);

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), groupsOf(provider, "alice", ALICE_DN));
    }
}
