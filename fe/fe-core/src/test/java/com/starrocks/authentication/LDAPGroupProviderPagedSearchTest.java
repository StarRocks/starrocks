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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.SizeLimitExceededException;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * RFC 2696 simple paged results handling. Without it, a directory whose server-side MaxPageSize is
 * smaller than the number of matching group entries answers with sizeLimitExceeded and the refresh
 * collects nothing.
 */
public class LDAPGroupProviderPagedSearchTest {

    private static final String BASE_DN = "DC=x,DC=y";
    private static final String FILTER = "(objectClass=group)";
    private static final String[] ATTRS = new String[] {"cn", "member"};

    private int originalMaxSearchPages;

    @BeforeEach
    public void setUp() {
        originalMaxSearchPages = LDAPGroupProvider.MAX_SEARCH_PAGES;
    }

    @AfterEach
    public void tearDown() {
        LDAPGroupProvider.MAX_SEARCH_PAGES = originalMaxSearchPages;
    }

    @Test
    public void testSinglePageWithoutCookieIssuesOneSearch() throws Exception {
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results("g1", "g2"));
        when(ctx.getResponseControls()).thenReturn(new Control[] {pagedResponse(2, null)});

        Assertions.assertEquals(List.of("g1", "g2"), collect(ctx, 1000));
        verify(ctx, times(1)).search(anyString(), anyString(), any(SearchControls.class));
    }

    @Test
    public void testServerWithoutPagingSupportStillReturnsEverything() throws Exception {
        // A server that does not implement RFC 2696 ignores a NONCRITICAL control and sends no response
        // control at all. That must collapse to exactly the single unpaged search this replaced.
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results("g1", "g2", "g3"));
        when(ctx.getResponseControls()).thenReturn(null);

        Assertions.assertEquals(List.of("g1", "g2", "g3"), collect(ctx, 1000));
        verify(ctx, times(1)).search(anyString(), anyString(), any(SearchControls.class));
    }

    @Test
    public void testCookieChainingWalksEveryPage() throws Exception {
        LdapContext ctx = mock(LdapContext.class);
        doReturn(results("g1", "g2"), results("g3"))
                .when(ctx).search(anyString(), anyString(), any(SearchControls.class));
        when(ctx.getResponseControls()).thenReturn(
                new Control[] {pagedResponse(2, new byte[] {0x01, 0x02})},
                new Control[] {pagedResponse(1, new byte[0])});

        Assertions.assertEquals(List.of("g1", "g2", "g3"), collect(ctx, 2));
        verify(ctx, times(2)).search(anyString(), anyString(), any(SearchControls.class));
    }

    @Test
    public void testRequestControlsAreClearedOnTheWayOut() throws Exception {
        // The paging control is scoped to the context. Leaving it in place would attach this search's
        // cookie to the base-object reads that range retrieval issues afterwards.
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results("g1"));
        when(ctx.getResponseControls()).thenReturn(new Control[] {pagedResponse(1, null)});

        collect(ctx, 1000);

        InOrder ordered = inOrder(ctx);
        ordered.verify(ctx).setRequestControls(notNull());
        ordered.verify(ctx).search(anyString(), anyString(), any(SearchControls.class));
        ordered.verify(ctx).setRequestControls(isNull());
    }

    @Test
    public void testRequestControlsAreClearedEvenWhenSearchFails() throws Exception {
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
                .thenThrow(new SizeLimitExceededException("boom"));

        Assertions.assertThrows(SizeLimitExceededException.class, () -> collect(ctx, 1000));
        verify(ctx).setRequestControls(isNull());
    }

    @Test
    public void testZeroPageSizeSendsNoControlAtAll() throws Exception {
        // The runtime kill switch: ldap_group_provider_search_page_size = 0 falls back to one plain search.
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results("g1"));

        Assertions.assertEquals(List.of("g1"), collect(ctx, 0));
        verify(ctx, never()).setRequestControls(any());
        verify(ctx, never()).getResponseControls();
        verify(ctx, times(1)).search(anyString(), anyString(), any(SearchControls.class));
    }

    @Test
    public void testRunawayCookieIsCappedByMaxSearchPages() throws Exception {
        LDAPGroupProvider.MAX_SEARCH_PAGES = 3;

        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
                .thenAnswer(invocation -> results("g"));
        // Always a non-empty cookie: only the safety limit can end this.
        when(ctx.getResponseControls()).thenReturn(new Control[] {pagedResponse(1, new byte[] {0x7f})});

        NamingException e = Assertions.assertThrows(NamingException.class, () -> collect(ctx, 1));
        Assertions.assertTrue(e.getMessage().contains("MAX_SEARCH_PAGES"), e.getMessage());
        verify(ctx, times(3)).search(anyString(), anyString(), any(SearchControls.class));
    }

    @Test
    public void testSubtreeScopeAndReturningAttributesArePassedThrough() throws Exception {
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results("g1"));

        collect(ctx, 0);

        ArgumentCaptor<SearchControls> captor = ArgumentCaptor.forClass(SearchControls.class);
        verify(ctx).search(anyString(), anyString(), captor.capture());
        Assertions.assertEquals(SearchControls.SUBTREE_SCOPE, captor.getValue().getSearchScope());
        Assertions.assertArrayEquals(ATTRS, captor.getValue().getReturningAttributes());
    }

    @Test
    public void testPagedResultsControlIsNonCritical() throws Exception {
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results("g1"));
        when(ctx.getResponseControls()).thenReturn(new Control[] {pagedResponse(1, null)});

        collect(ctx, 250);

        // Two calls: the control on the way in, then the null that clears it on the way out.
        ArgumentCaptor<Control[]> captor = ArgumentCaptor.forClass(Control[].class);
        verify(ctx, times(2)).setRequestControls(captor.capture());
        Control[] sent = captor.getAllValues().get(0);
        Assertions.assertEquals(1, sent.length);
        Assertions.assertTrue(sent[0] instanceof PagedResultsControl);
        // NONCRITICAL is what makes a server without RFC 2696 support ignore this instead of failing.
        Assertions.assertFalse(sent[0].isCritical());
    }

    @Test
    public void testEveryPageEnumerationIsClosed() throws Exception {
        ListNamingEnumeration page1 = results("g1");
        ListNamingEnumeration page2 = results("g2");

        LdapContext ctx = mock(LdapContext.class);
        doReturn(page1, page2).when(ctx).search(anyString(), anyString(), any(SearchControls.class));
        when(ctx.getResponseControls()).thenReturn(
                new Control[] {pagedResponse(1, new byte[] {0x01})},
                new Control[] {pagedResponse(1, new byte[0])});

        collect(ctx, 1);

        Assertions.assertTrue(page1.closed, "first page enumeration must be closed");
        Assertions.assertTrue(page2.closed, "second page enumeration must be closed");
    }

    // ---------- unfollowable referrals (PartialResultException) ----------

    @Test
    public void testReferralOnTheFinalPageIsTolerated() throws Exception {
        // With the domain root as base DN, AD routinely surfaces CN=Configuration and friends as referrals
        // the provider will not chase, after the real entries. Failing the refresh here would fail every
        // healthy refresh on such a deployment.
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
                .thenReturn(referralAfter("g1", "g2"));
        when(ctx.getResponseControls()).thenReturn(new Control[] {pagedResponse(2, new byte[0])});

        List<String> seen = new ArrayList<>();
        boolean complete = LDAPGroupProvider.searchGroupEntriesPaged(ctx, BASE_DN, FILTER, ATTRS, 1000,
                result -> seen.add(result.getName()));

        Assertions.assertTrue(complete, "a referral with no pages pending must not fail the refresh");
        Assertions.assertEquals(List.of("g1", "g2"), seen);
    }

    @Test
    public void testReferralWithPagesStillPendingReportsIncomplete() throws Exception {
        // Same exception, different meaning: the aborted page still had a continuation cookie, so group
        // entries were genuinely lost. Publishing that as a complete read is what the cache policy exists
        // to prevent - a partial map is indistinguishable from "these users are in no groups".
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
                .thenReturn(referralAfter("g1"));
        when(ctx.getResponseControls()).thenReturn(new Control[] {pagedResponse(1, new byte[] {0x01, 0x02})});

        List<String> seen = new ArrayList<>();
        boolean complete = LDAPGroupProvider.searchGroupEntriesPaged(ctx, BASE_DN, FILTER, ATTRS, 1,
                result -> seen.add(result.getName()));

        Assertions.assertFalse(complete, "entries were lost, the caller must not publish this");
        // The entries seen before the referral are still delivered; it is the refresh as a whole that is void.
        Assertions.assertEquals(List.of("g1"), seen);
        verify(ctx).setRequestControls(isNull());
    }

    @Test
    public void testUnreadableResponseControlsAfterReferralFailClosed() throws Exception {
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
                .thenReturn(referralAfter("g1"));
        when(ctx.getResponseControls()).thenThrow(new NamingException("controls unavailable"));

        Assertions.assertFalse(
                LDAPGroupProvider.searchGroupEntriesPaged(ctx, BASE_DN, FILTER, ATTRS, 1, result -> { }),
                "if we cannot tell whether pages remained, assume entries were lost");
    }

    @Test
    public void testReferralOnAnUnpagedSearchIsTolerated() throws Exception {
        // pageSize <= 0 means there is no pagination to lose, so the old tolerate-and-continue behaviour
        // is still the right one.
        LdapContext ctx = mock(LdapContext.class);
        when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
                .thenReturn(referralAfter("g1", "g2"));

        List<String> seen = new ArrayList<>();
        Assertions.assertTrue(LDAPGroupProvider.searchGroupEntriesPaged(ctx, BASE_DN, FILTER, ATTRS, 0,
                result -> seen.add(result.getName())));
        Assertions.assertEquals(List.of("g1", "g2"), seen);
        verify(ctx, never()).getResponseControls();
    }

    @Test
    public void testExtractPagedResultsCookie() throws IOException {
        Assertions.assertNull(LDAPGroupProvider.extractPagedResultsCookie(null));
        Assertions.assertNull(LDAPGroupProvider.extractPagedResultsCookie(new Control[0]));
        // An empty cookie means the result set is complete.
        Assertions.assertNull(LDAPGroupProvider.extractPagedResultsCookie(
                new Control[] {pagedResponse(1, new byte[0])}));
        Assertions.assertArrayEquals(new byte[] {0x0a},
                LDAPGroupProvider.extractPagedResultsCookie(new Control[] {pagedResponse(1, new byte[] {0x0a})}));
    }

    // ---------- helpers ----------

    private static List<String> collect(LdapContext ctx, int pageSize) throws NamingException, IOException {
        List<String> seen = new ArrayList<>();
        LDAPGroupProvider.searchGroupEntriesPaged(ctx, BASE_DN, FILTER, ATTRS, pageSize,
                result -> seen.add(result.getName()));
        return seen;
    }

    /**
     * Build a real PagedResultsResponseControl rather than mocking it, so the JDK's own BER parsing is
     * exercised. The control value is {@code SEQUENCE { size INTEGER, cookie OCTET STRING }}.
     */
    private static PagedResultsResponseControl pagedResponse(int size, byte[] cookie) throws IOException {
        byte[] safeCookie = cookie == null ? new byte[0] : cookie;
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        // INTEGER size - the values used here stay well inside a single byte.
        body.write(new byte[] {0x02, 0x01, (byte) size});
        body.write(0x04);
        body.write(safeCookie.length);
        body.write(safeCookie);

        byte[] bodyBytes = body.toByteArray();
        ByteArrayOutputStream value = new ByteArrayOutputStream();
        value.write(0x30);
        value.write(bodyBytes.length);
        value.write(bodyBytes);

        return new PagedResultsResponseControl(PagedResultsResponseControl.OID, Control.NONCRITICAL,
                value.toByteArray());
    }

    private static ListNamingEnumeration results(String... names) {
        return new ListNamingEnumeration(searchResults(names), false);
    }

    /**
     * An enumeration that yields {@code names} and then raises PartialResultException, the way the LDAP
     * provider reports a referral it will not chase.
     */
    private static ListNamingEnumeration referralAfter(String... names) {
        return new ListNamingEnumeration(searchResults(names), true);
    }

    private static List<SearchResult> searchResults(String... names) {
        List<SearchResult> list = new ArrayList<>();
        for (String name : names) {
            list.add(new SearchResult(name, null, new BasicAttributes(), true));
        }
        return list;
    }

    /** Minimal NamingEnumeration over a fixed list, recording close() so it can be asserted on. */
    private static final class ListNamingEnumeration implements NamingEnumeration<SearchResult> {
        private final List<SearchResult> items;
        private final boolean referralAtEnd;
        private int index;
        private boolean closed;

        private ListNamingEnumeration(List<SearchResult> items, boolean referralAtEnd) {
            this.items = items;
            this.referralAtEnd = referralAtEnd;
        }

        @Override
        public SearchResult next() {
            return items.get(index++);
        }

        @Override
        public boolean hasMore() throws NamingException {
            if (index >= items.size() && referralAtEnd) {
                throw new PartialResultException("unprocessed continuation reference(s)");
            }
            return index < items.size();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean hasMoreElements() {
            try {
                return hasMore();
            } catch (NamingException e) {
                return false;
            }
        }

        @Override
        public SearchResult nextElement() {
            return next();
        }
    }
}
